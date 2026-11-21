import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  Environment,
  Session,
} from "@open-managed-agents/managed-agents-application";
import type {
  SessionExecutionContextSourcePort,
} from "@open-managed-agents/session-runtime-contract/context";
import {
  ensureSessionExecutionCoordinatorSchema,
  SqlSessionExecutionCoordinator,
} from "@open-managed-agents/session-runtime-sql/coordination";
import { planSessionContinuation } from "@open-managed-agents/session-runtime-contract/continuation";
import { encodeSessionEventDocument } from "@open-managed-agents/session-runtime-contract/history";
import { loadSessionContinuationSnapshot } from "@open-managed-agents/session-runtime-sql/continuation";
import { createBetterSqlite3SqlClient } from "@open-managed-agents/sql-client";
import { ConnectionLost, Deadlock } from "@open-managed-agents/sql-client/db-errors";
import {
  NodeSessionExecutionWorker,
  type NodeSessionExecutionRuntime,
  type RunNodeSessionExecution,
} from "../../../packages/control-plane-node/src/lib/node-session-execution-worker";

import { listClaimScopes } from "../../../packages/control-plane-node/src/lib/tank-retained/startup-recovery";

const session: Session = {
  id: "session_01",
  agent: {
    id: "agent_01",
    description: null,
    mcpServers: [],
    model: { id: "model_01" },
    multiagent: null,
    name: "Agent",
    skills: [],
    system: null,
    tools: [],
    version: 1,
  },
  archivedAt: null,
  budget: null,
  createdAt: "2026-09-04T00:00:00.000Z",
  environmentId: "env_01",
  metadata: {},
  outcomeEvaluations: [],
  resources: [],
  stats: {},
  status: "idle",
  title: "Session",
  updatedAt: "2026-09-04T00:00:00.000Z",
  usage: {},
  vaultIds: [],
};

const environment: Environment = {
  id: "env_01",
  archivedAt: null,
  config: { type: "cloud", networking: { type: "unrestricted" }, packages: {
    apt: [], cargo: [], gem: [], go: [], npm: [], pip: [],
  } },
  createdAt: "2026-09-04T00:00:00.000Z",
  description: null,
  metadata: {},
  name: "Environment",
  updatedAt: "2026-09-04T00:00:00.000Z",
};

function accepted(id: string, type: "user.message" | "user.interrupt" = "user.message") {
  const base = {
    workspaceId: "workspace_01",
    sessionId: session.id,
    session,
    environment,
  };
  return type === "user.interrupt"
    ? {
        ...base,
        events: [{
          id,
          type,
          processedAt: "2026-09-04T00:00:03.000Z",
        }] as const,
      }
    : {
        ...base,
        events: [{
          id,
          type,
          content: [{ type: "text" as const, text: id }],
          processedAt: "2026-09-04T00:00:01.000Z",
        }] as const,
      };
}

describe("NodeSessionExecutionWorker", () => {
  let now: Date;
  let sql: Awaited<ReturnType<typeof createBetterSqlite3SqlClient>>;
  let coordinator: SqlSessionExecutionCoordinator;
  let context: SessionExecutionContextSourcePort;
  let runs: RunNodeSessionExecution[];
  let cancellations: string[];
  let runtime: NodeSessionExecutionRuntime;

  beforeEach(async () => {
    now = new Date("2026-09-04T00:00:02.000Z");
    sql = await createBetterSqlite3SqlClient(":memory:");
    await ensureSessionExecutionCoordinatorSchema(sql);
    coordinator = new SqlSessionExecutionCoordinator(sql);
    context = {
      find: async () => ({ session, environment, revision: 1 }),
    };
    runs = [];
    cancellations = [];
    runtime = {
      run: async (input) => { runs.push(structuredClone(input)); },
      cancel: async (input) => { cancellations.push(input.reason); },
    };
  });

  function worker(ownerId = "node_01", overrides: Partial<ConstructorParameters<typeof NodeSessionExecutionWorker>[0]> = {}) {
    let attempt = 0;
    return new NodeSessionExecutionWorker({
      coordinator,
      context,
      runtime,
      ownerId,
      clock: { now: () => now },
      ids: { nextAttemptId: () => `${ownerId}_attempt_${++attempt}` },
      leaseTtlMs: 30_000,
      heartbeatIntervalMs: 10_000,
      maxConcurrent: 4,
      ...overrides,
    });
  }

  it("durably admits a Managed Event batch and runs it with fresh canonical context", async () => {
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("event_01"));
    await executor.waitForIdle();

    expect(runs).toHaveLength(1);
    expect(runs[0]).toMatchObject({
      workspaceId: "workspace_01",
      sessionId: "session_01",
      executionId: "event_01",
      session,
      environment,
      events: [{ id: "event_01" }],
      fence: { ownerId: "node_01", generation: 1 },
    });
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_01",
    })).resolves.toMatchObject({ state: "completed" });
  });

  it("admits a threaded event into its own execution lane", async () => {
    const executor = worker();
    await executor.sessionEventsAccepted({
      ...accepted("thread_event"),
      events: [{
        ...accepted("thread_event").events[0],
        sessionThreadId: "sthr_child",
      }],
    });
    await executor.waitForIdle();

    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "thread_event",
    })).resolves.toMatchObject({ laneId: "sthr_child" });
  });

  it("lets only one replica execute a durably admitted batch", async () => {
    await coordinator.admit({
      execution: {
        id: "event_01",
        workspaceId: "workspace_01",
        sessionId: "session_01",
        admittedAt: "2026-09-04T00:00:01.000Z",
        events: [...accepted("event_01").events],
      },
    });
    const nodeA = worker("node_a");
    const nodeB = worker("node_b");
    await Promise.all([nodeA.poll(), nodeB.poll()]);
    await Promise.all([nodeA.waitForIdle(), nodeB.waitForIdle()]);

    expect(runs).toHaveLength(1);
    expect(runs[0]?.fence.ownerId).toMatch(/^node_[ab]$/u);
  });

  it("recovers work whose previous owner lost its lease", async () => {
    await coordinator.admit({
      execution: {
        id: "event_01",
        workspaceId: "workspace_01",
        sessionId: "session_01",
        admittedAt: "2026-09-04T00:00:00.000Z",
        events: [...accepted("event_01").events],
      },
    });
    await coordinator.claim({
      ownerId: "dead_node",
      attemptId: "dead_attempt",
      claimedAt: "2026-09-04T00:00:00.000Z",
      leaseTtlMs: 1_000,
    });
    now = new Date("2026-09-04T00:00:02.000Z");

    const replacement = worker("replacement");
    await replacement.poll();
    await replacement.waitForIdle();

    expect(runs[0]?.fence).toMatchObject({
      ownerId: "replacement",
      generation: 2,
    });
  });

  it("recovers the exact local attempt only after runtime, cancellation and retained VM drain", async () => {
    await sql.exec(`
      CREATE TABLE managed_sessions (workspace_id TEXT, id TEXT, revision INTEGER, archived_at INTEGER);
      CREATE TABLE managed_session_initial_events (workspace_id TEXT, session_id TEXT, document TEXT, sequence INTEGER);
      CREATE TABLE managed_session_events (workspace_id TEXT, session_id TEXT, document TEXT, processed_at INTEGER, id TEXT);
      INSERT INTO managed_sessions VALUES ('workspace_01', 'session_01', 1, NULL);
      INSERT INTO managed_sessions VALUES ('workspace_01', 'another_session', 1, NULL);
    `);
    coordinator = new SqlSessionExecutionCoordinator(sql, {
      callbackContinuations: true,
      serializeSessionClaims: true,
    });
    const running = Promise.withResolvers<void>();
    const runtimeDrained = Promise.withResolvers<void>();
    const cancelling = Promise.withResolvers<void>();
    const cancellationDrained = Promise.withResolvers<void>();
    const quiescing = Promise.withResolvers<void>();
    const quiesced = Promise.withResolvers<void>();
    const plans: string[] = [];
    runtime.run = async (input) => {
      const snapshot = await loadSessionContinuationSnapshot(sql, input);
      plans.push(planSessionContinuation(snapshot, input.executionId).mode);
      runs.push(structuredClone(input));
      if (runs.length === 1) {
        running.resolve();
        await runtimeDrained.promise;
      }
    };
    runtime.cancel = async ({ reason }) => {
      cancellations.push(reason);
      cancelling.resolve();
      await cancellationDrained.promise;
    };
    const executor = worker("owner:arbitrary", {
      maxConcurrent: 1,
      heartbeatIntervalMs: 10,
      leaseTtlMs: 100,
      quiesceLostExecution: async (input) => {
        expect(input.fence).toMatchObject({ executionId: "original", generation: 1 });
        expect(input).not.toHaveProperty("currentFence");
        quiescing.resolve();
        await quiesced.promise;
      },
    });

    await executor.sessionEventsAccepted(accepted("original"));
    await running.promise;
    const completedOutput = {
      id: "committed_idle",
      type: "session.status_idle",
      stopReason: { type: "end_turn" },
      processedAt: now.toISOString(),
    } as const;
    await sql.prepare(`INSERT INTO managed_session_events VALUES (?, ?, ?, ?, ?)`)
      .bind("workspace_01", session.id,
        encodeSessionEventDocument(completedOutput, { revision: 1, index: 0 }, "original"),
        now.getTime(), completedOutput.id)
      .run();
    await executor.sessionEventsAccepted({ ...accepted("a_other"), sessionId: "another_session" });
    now = new Date(now.getTime() + 100);
    await cancelling.promise;
    await executor.poll();
    expect(runs.map(({ executionId }) => executionId)).toEqual(["original"]);

    runtimeDrained.resolve();
    await executor.poll();
    expect(runs.map(({ executionId }) => executionId)).toEqual(["original"]);
    cancellationDrained.resolve();
    await quiescing.promise;
    await expect(coordinator.find({ workspaceId: "workspace_01", executionId: "original" }))
      .resolves.toMatchObject({ state: "running", attemptCount: 1, attempt: { generation: 1 } });
    await expect(coordinator.find({ workspaceId: "workspace_01", executionId: "a_other" }))
      .resolves.toMatchObject({ state: "queued", attemptCount: 0 });
    expect(runs.map(({ executionId }) => executionId)).toEqual(["original"]);
    quiesced.resolve();
    await executor.waitForIdle();

    expect(cancellations).toEqual(["lease_lost"]);
    expect(plans).toEqual(["model", "finalize", "model"]);
    expect(runs.map(({ executionId, fence }) => [executionId, fence.ownerId, fence.generation])).toEqual([
      ["original", "owner:arbitrary", 1],
      ["original", "owner:arbitrary", 2],
      ["a_other", "owner:arbitrary", 1],
    ]);
    expect(runs[1]?.fence.attemptId).not.toBe(runs[0]?.fence.attemptId);
  });

  it("releases the slot after proof when a competing owner replaced the attempt", async () => {
    const running = Promise.withResolvers<void>();
    const runtimeDrained = Promise.withResolvers<void>();
    const cancelling = Promise.withResolvers<void>();
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      if (runs.length === 1) {
        running.resolve();
        await runtimeDrained.promise;
      }
    };
    runtime.cancel = async () => { cancelling.resolve(); };
    const executor = worker("node_a", {
      maxConcurrent: 1,
      heartbeatIntervalMs: 10,
      leaseTtlMs: 100,
      quiesceLostExecution: async () => {},
    });
    await executor.sessionEventsAccepted(accepted("original"));
    await running.promise;
    now = new Date(now.getTime() + 100);
    const competing = await coordinator.claim({
      ownerId: "node_b",
      attemptId: "competing",
      claimedAt: now.toISOString(),
      leaseTtlMs: 100,
    });
    expect(competing.type).toBe("claimed");
    if (competing.type !== "claimed") throw new Error("Competing claim failed");
    await executor.sessionEventsAccepted({ ...accepted("a_other"), sessionId: "another_session" });
    await cancelling.promise;
    runtimeDrained.resolve();
    await executor.waitForIdle();
    expect(runs.map(({ executionId }) => executionId)).toEqual(["original", "a_other"]);
  });

  it.each([
    [1, false],
    [2, false],
    [1, true],
  ])("retries physical proof with %i slots (interrupted: %s)", async (maxConcurrent, interrupted) => {
    const running = Promise.withResolvers<void>();
    const runtimeDrained = Promise.withResolvers<void>();
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      if (input.executionId === "original" && input.fence.generation === 1) {
        running.resolve();
        await runtimeDrained.promise;
      }
    };
    let checks = 0;
    const errors: string[] = [];
    const executor = worker("node_a", {
      maxConcurrent,
      listClaimScopes: (claimedAt) => listClaimScopes(sql, claimedAt),
      pollIntervalMs: 50,
      onError: (error) => errors.push(error.message),
      quiesceLostExecution: async (input) => {
        expect(input.fence.generation).toBe(1);
        expect(input).not.toHaveProperty("currentFence");
        if (++checks <= 3) throw new Error("quiescence unknown");
      },
    });
    const original = accepted("original");
    await coordinator.admit({
      execution: {
        id: "original",
        workspaceId: original.workspaceId,
        sessionId: original.sessionId,
        admittedAt: now.toISOString(),
        events: [...original.events],
      },
      policy: { maxAttempts: 2, timeoutMs: 120_000 },
    });
    await executor.poll();
    await running.promise;
    now = new Date(now.getTime() + 30_000);
    runtimeDrained.resolve();
    await executor.waitForIdle();
    await executor.sessionEventsAccepted({ ...accepted("a_other"), sessionId: "another_session" });
    expect(checks).toBe(1);
    const beforeRecovery = maxConcurrent === 1 ? ["original"] : ["original", "a_other"];
    const findOriginal = () => coordinator.find({ workspaceId: original.workspaceId, executionId: "original" });
    for (const count of [2, 3]) {
      await vi.waitFor(async () => {
        await executor.poll();
        expect(checks).toBe(count);
      }, { interval: 5 });
      await expect(findOriginal()).resolves.toMatchObject({ state: "running", attemptCount: 1 });
      expect(runs.map(({ executionId }) => executionId)).toEqual(beforeRecovery);
    }
    if (interrupted) {
      const interrupt = accepted("interrupt", "user.interrupt");
      await executor.sessionEventsAccepted({
        ...interrupt,
        events: [{ ...interrupt.events[0], processedAt: now.toISOString() }],
      });
      await expect(findOriginal()).resolves.toMatchObject({ state: "cancelled", attemptCount: 1 });
      expect(runs).toHaveLength(1);
    }
    await vi.waitFor(async () => {
      await executor.waitForIdle();
      expect(runs).toHaveLength(interrupted ? 2 : 3);
    }, { interval: 5 });
    expect(errors).toEqual(Array(3).fill("quiescence unknown"));
    await expect(findOriginal()).resolves.toMatchObject(interrupted
      ? { state: "cancelled", attemptCount: 1 }
      : { state: "completed", attemptCount: 2 });
    expect(runs.slice(1).map(({ executionId, fence }) => [executionId, fence.generation]))
      .toEqual(interrupted ? [["a_other", 1]] : maxConcurrent === 1
        ? [["original", 2], ["a_other", 1]]
        : [["a_other", 1], ["original", 2]]);
  });

  it("records attempt exhaustion while retaining the slot of an unconfirmed guest", async () => {
    const running = Promise.withResolvers<void>();
    const drained = Promise.withResolvers<void>();
    const stop = Promise.withResolvers<void>();
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      if (input.executionId === "original") {
        running.resolve();
        await drained.promise;
      }
    };
    const executor = worker("node_a", {
      maxConcurrent: 2,
      listClaimScopes: (claimedAt) => listClaimScopes(sql, claimedAt),
      quiesceLostExecution: () => stop.promise,
    });
    const original = accepted("original");
    await coordinator.admit({
      execution: {
        id: "original",
        workspaceId: original.workspaceId,
        sessionId: original.sessionId,
        admittedAt: now.toISOString(),
        events: [...original.events],
      },
      policy: { maxAttempts: 1, timeoutMs: 120_000 },
    });
    await executor.poll();
    await running.promise;
    now = new Date(now.getTime() + 30_000);
    drained.resolve();
    try {
      await vi.waitFor(async () => {
        await executor.poll();
        await expect(coordinator.find({ workspaceId: original.workspaceId, executionId: "original" }))
          .resolves.toMatchObject({ state: "failed", failure: "execution attempt limit exhausted" });
      });
      await executor.sessionEventsAccepted(accepted("same_guest"));
      await executor.sessionEventsAccepted({ ...accepted("free_slot"), sessionId: "another_session" });
      await executor.waitForIdle();
      expect(runs.map(({ executionId }) => executionId)).toEqual(["original", "free_slot"]);
      await expect(coordinator.find({ workspaceId: original.workspaceId, executionId: "same_guest" }))
        .resolves.toMatchObject({ state: "queued", attemptCount: 0 });
    } finally {
      executor.stop();
      stop.reject(new Error("retained_recovery_stop_unconfirmed"));
      await executor.waitForIdle();
    }
  });

  it.each([1, 2])("reserves a surviving startup guest with %i slots and a fresh owner incarnation", async (maxConcurrent) => {
    const original = accepted("original");
    await coordinator.admit({
      execution: {
        id: "original",
        workspaceId: original.workspaceId,
        sessionId: original.sessionId,
        admittedAt: now.toISOString(),
        events: [...original.events],
      },
      policy: { maxAttempts: 2, timeoutMs: 120_000 },
    });
    await coordinator.claim({
      ownerId: "configured_owner:old",
      attemptId: "old_attempt",
      claimedAt: now.toISOString(),
      leaseTtlMs: 1_000,
    });
    now = new Date(now.getTime() + 1_000);
    const quiesce = vi.fn(async () => {
      throw new Error("retained_recovery_stop_unconfirmed");
    });
    const unrelated = { ...accepted("a_other"), sessionId: "another_session" };
    const executor = worker("configured_owner:new", {
      maxConcurrent,
      pollIntervalMs: 25,
      listClaimScopes: (claimedAt) => listClaimScopes(sql, claimedAt),
      discoverStartupResidents: async () => [{
        workspaceId: original.workspaceId, sessionId: original.sessionId, quiesce,
      }],
    });
    await executor.sessionEventsAccepted(unrelated);
    await executor.waitForIdle();
    expect(runs.map(({ executionId }) => executionId)).toEqual(maxConcurrent === 1 ? [] : ["a_other"]);
    await expect(coordinator.find({ workspaceId: original.workspaceId, executionId: "original" }))
      .resolves.toMatchObject({ state: "running", attemptCount: 1 });

    quiesce.mockResolvedValue(undefined);
    await vi.waitFor(async () => {
      await executor.waitForIdle();
      expect(runs).toHaveLength(2);
    }, { interval: 5 });
    expect(runs.find(({ executionId }) => executionId === "original")?.fence)
      .toMatchObject({ ownerId: "configured_owner:new", generation: 2 });
  });

  it("reuses the exact recovery claim after its acknowledgement is lost", async () => {
    const drained = Promise.withResolvers<void>();
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      if (input.fence.generation === 1) await drained.promise;
    };
    const quiesce = vi.fn(async () => {});
    const claim = coordinator.claim.bind(coordinator);
    const recoveryIds: string[] = [];
    vi.spyOn(coordinator, "claim").mockImplementation(async (input) => {
      const result = await claim(input);
      if (input.recoverFrom !== undefined) {
        recoveryIds.push(input.attemptId);
        if (recoveryIds.length === 1) {
          expect(result.type).toBe("claimed");
          throw new Error("claim acknowledgement lost");
        }
      }
      return result;
    });
    const executor = worker("node_a", { maxConcurrent: 1, pollIntervalMs: 20, quiesceLostExecution: quiesce });
    await executor.sessionEventsAccepted(accepted("original"));
    await vi.waitFor(() => expect(runs).toHaveLength(1), { interval: 5 });
    now = new Date(now.getTime() + 30_000);
    drained.resolve();
    await vi.waitFor(async () => {
      await executor.waitForIdle();
      expect(runs.map(({ fence }) => fence.generation)).toEqual([1, 2]);
    }, { interval: 5 });
    expect(recoveryIds).toHaveLength(2);
    expect(recoveryIds[1]).toBe(recoveryIds[0]);
    expect(quiesce).toHaveBeenCalledTimes(1);
  });

  it("reconciles a committed settlement with a missing acknowledgement", async () => {
    const withheld = Promise.withResolvers<void>();
    const committed = Promise.withResolvers<void>();
    const executor = worker("node_a", {
      maxConcurrent: 1,
      heartbeatIntervalMs: 10,
      leaseTtlMs: 100,
      quiesceLostExecution: async () => {},
      coordinator: {
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: async (input) => {
          const result = await coordinator.settle(input);
          if (input.fence.executionId === "original") {
            committed.resolve();
            await withheld.promise;
          }
          return result;
        },
      },
    });
    await executor.sessionEventsAccepted(accepted("original"));
    await committed.promise;
    await executor.sessionEventsAccepted({ ...accepted("a_other"), sessionId: "another_session" });
    try {
      await executor.waitForIdle();
      expect(runs.map(({ executionId }) => executionId)).toEqual(["original", "a_other"]);
      await expect(coordinator.find({ workspaceId: "workspace_01", executionId: "original" }))
        .resolves.toMatchObject({ state: "completed", attemptCount: 1 });
    } finally {
      withheld.resolve();
    }
  });

  it("turns a cross-replica interrupt into a durable signal and local cancellation", async () => {
    let release: (() => void) | undefined;
    const running = new Promise<void>((resolve) => { release = resolve; });
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await running;
    };
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("event_01"));
    await Promise.resolve();

    await executor.sessionEventsAccepted(accepted("interrupt_01", "user.interrupt"));
    expect(cancellations).toEqual(["interrupt_requested"]);
    release?.();
    await executor.waitForIdle();

    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_01",
    })).resolves.toMatchObject({ state: "cancelled" });
  });

  it("cancels only the thread lane named by an interrupt", async () => {
    const releases: Array<() => void> = [];
    const cancelledExecutions: string[] = [];
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await new Promise<void>((resolve) => releases.push(resolve));
    };
    runtime.cancel = async (input) => {
      cancelledExecutions.push(input.executionId);
    };
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("primary"));
    await executor.sessionEventsAccepted({
      ...accepted("sibling"),
      events: [{
        ...accepted("sibling").events[0],
        sessionThreadId: "sthr_sibling",
      }],
    });
    await Promise.resolve();

    await executor.sessionEventsAccepted({
      ...accepted("interrupt_primary", "user.interrupt"),
      events: [{
        ...accepted("interrupt_primary", "user.interrupt").events[0],
        sessionThreadId: "sthr_primary",
      }],
    });

    expect(cancelledExecutions).toEqual(["primary"]);
    for (const release of releases) release();
    await executor.waitForIdle();
  });

  it("cancels the runtime and refuses to settle after losing its fence", async () => {
    let release: (() => void) | undefined;
    const running = new Promise<void>((resolve) => { release = resolve; });
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await running;
    };
    const executor = worker("node_01", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async () => ({ type: "lost" }),
      },
    });
    await executor.sessionEventsAccepted(accepted("event_01"));
    await new Promise((resolve) => setTimeout(resolve, 15));

    expect(cancellations).toContain("lease_lost");
    release?.();
    await executor.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_01",
    })).resolves.toMatchObject({ state: "running" });
  });

  it("keeps a healthy turn when renew fails transiently", async () => {
    let release: (() => void) | undefined;
    const running = new Promise<void>((resolve) => { release = resolve; });
    const backgroundErrors: Error[] = [];
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await running;
    };
    const executor = worker("node_01", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      onError: (error) => backgroundErrors.push(error),
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async () => { throw new Error("database unavailable"); },
      },
    });
    await executor.sessionEventsAccepted(accepted("event_01"));
    await new Promise((resolve) => setTimeout(resolve, 15));

    expect(cancellations).not.toContain("lease_lost");
    expect(backgroundErrors.map((error) => error.message)).toContain("database unavailable");
    release?.();
    await executor.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_01",
    })).resolves.toMatchObject({ state: "completed" });
  });

  it("supervised fibers keep a turn alive across deadlock and connection loss", async () => {
    const rejections: unknown[] = [];
    const onRejection = (reason: unknown) => { rejections.push(reason); };
    process.on("unhandledRejection", onRejection);
    let release: (() => void) | undefined;
    const running = new Promise<void>((resolve) => { release = resolve; });
    const backgroundErrors: Error[] = [];
    let renews = 0;
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await running;
    };
    const executor = worker("node_fiber", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 30_000,
      onError: (error) => backgroundErrors.push(error),
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async () => {
          renews += 1;
          if (renews === 1) {
            throw new Deadlock({
              op: "session_execution.renew",
              sessionId: session.id,
              cause: new Error("1213"),
            });
          }
          throw new ConnectionLost({
            op: "session_execution.renew",
            sessionId: session.id,
            cause: new Error("ECONNRESET"),
          });
        },
      },
    });
    try {
      await executor.sessionEventsAccepted(accepted("event_fiber"));
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(renews).toBeGreaterThan(0);
      expect(cancellations).not.toContain("lease_lost");
      expect(rejections).toEqual([]);
      expect(backgroundErrors.map((error) => (error as { _tag?: string })._tag)).toEqual(
        expect.arrayContaining(["Deadlock", "ConnectionLost"]),
      );
      release?.();
      await executor.waitForIdle();
      await expect(coordinator.find({
        workspaceId: "workspace_01",
        executionId: "event_fiber",
      })).resolves.toMatchObject({ state: "completed" });
    } finally {
      process.off("unhandledRejection", onRejection);
    }
  });

  it("self-fences when a database partition leaves renewal hanging", async () => {
    let release: (() => void) | undefined;
    const running = new Promise<void>((resolve) => { release = resolve; });
    let renewalStarted!: () => void;
    const renewing = new Promise<void>((resolve) => { renewalStarted = resolve; });
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await running;
    };
    const executor = worker("node_01", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async () => {
          renewalStarted();
          await new Promise<never>(() => {});
        },
      },
    });

    await executor.sessionEventsAccepted(accepted("event_partitioned"));
    await renewing;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(cancellations).not.toContain("lease_lost");
    now = new Date(Date.parse(now.toISOString()) + 60_000);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(cancellations).toContain("lease_lost");
    release?.();
    await executor.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_partitioned",
    })).resolves.toMatchObject({ state: "running" });
  });

  it("validates scheduler bounds and makes start/stop idempotent", async () => {
    expect(() => worker("node_01", {
      heartbeatIntervalMs: 30_000,
      leaseTtlMs: 30_000,
    })).toThrow("heartbeat must be shorter");
    expect(() => worker("node_01", { maxConcurrent: 0 })).toThrow(
      "maxConcurrent must be a positive integer",
    );
    expect(() => worker("node_01", { maxConcurrent: 1.5 })).toThrow(
      "maxConcurrent must be a positive integer",
    );

    const errors: Error[] = [];
    const executor = worker("node_01", {
      pollIntervalMs: 2,
      onError: (error) => errors.push(error),
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        claim: async () => { throw "poll failed"; },
      },
    });
    executor.start();
    executor.start();
    await new Promise((resolve) => setTimeout(resolve, 8));
    executor.stop();
    executor.stop();
    expect(errors.some((error) => error.message === "poll failed")).toBe(true);
  });

  it("applies the documented scheduler defaults", () => {
    expect(() => new NodeSessionExecutionWorker({
      coordinator,
      context,
      runtime,
      ownerId: "default_worker",
      clock: { now: () => now },
      ids: { nextAttemptId: () => "default_attempt" },
    })).not.toThrow();
  });

  it("timestamps unprocessed input and interrupts with the worker clock", async () => {
    const executor = worker();
    await executor.sessionEventsAccepted({
      ...accepted("untimestamped"),
      events: [{
        id: "untimestamped",
        type: "user.message",
        content: [{ type: "text", text: "untimestamped" }],
      }],
    } as Parameters<typeof executor.sessionEventsAccepted>[0]);
    await executor.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "untimestamped",
    })).resolves.toMatchObject({ admittedAt: now.toISOString() });

    await executor.sessionEventsAccepted({
      ...accepted("untimestamped_interrupt", "user.interrupt"),
      events: [{ id: "untimestamped_interrupt", type: "user.interrupt" }],
    } as Parameters<typeof executor.sessionEventsAccepted>[0]);
  });

  it("does not cancel active work belonging to another session", async () => {
    let release: (() => void) | undefined;
    runtime.run = async () => {
      await new Promise<void>((resolve) => { release = resolve; });
    };
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("event_other_session"));
    await Promise.resolve();

    await executor.cancelSession({
      workspaceId: "workspace_01",
      sessionId: "different_session",
      reason: "unrelated cancellation",
    });

    expect(cancellations).toEqual([]);
    release?.();
    await executor.waitForIdle();
  });

  it("settles a runtime rejection caused by cancellation as cancelled", async () => {
    let rejectRun: ((error: Error) => void) | undefined;
    runtime.run = async () => await new Promise<void>((_resolve, reject) => {
      rejectRun = reject;
    });
    runtime.cancel = async (input) => {
      cancellations.push(input.reason);
      rejectRun?.(new Error("runtime cancelled"));
    };
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("event_cancel_reject"));
    await Promise.resolve();

    await executor.cancelSession({
      workspaceId: "workspace_01",
      sessionId: "session_01",
      reason: "stop",
    });
    await executor.waitForIdle();

    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_cancel_reject",
    })).resolves.toMatchObject({ state: "cancelled" });
  });

  it("reports non-Error polling failures without trusting observability hooks", async () => {
    const executor = worker("node_report", {
      onError: () => { throw new Error("observer failed"); },
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        claim: async () => { throw "string polling failure"; },
      },
    });

    await expect(executor.poll()).rejects.toBe("string polling failure");
  });

  it("reports cancellation failure after an indeterminate heartbeat", async () => {
    let release: (() => void) | undefined;
    const errors: Error[] = [];
    runtime.run = async () => {
      await new Promise<void>((resolve) => { release = resolve; });
    };
    runtime.cancel = async () => { throw new Error("cancel failed"); };
    const executor = worker("node_cancel_failure", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      onError: (error) => errors.push(error),
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async () => ({ type: "lost" as const }),
      },
    });
    await executor.sessionEventsAccepted(accepted("event_cancel_failure"));
    await new Promise((resolve) => setTimeout(resolve, 15));

    expect(errors.map((error) => error.message)).toEqual(
      expect.arrayContaining(["cancel failed"]),
    );
    release?.();
    await executor.waitForIdle();
  });

  it("keeps the newer active handle if a faulty coordinator duplicates a claim", async () => {
    await coordinator.admit({
      execution: {
        id: "duplicate_claim",
        workspaceId: "workspace_01",
        sessionId: "session_01",
        admittedAt: "2026-09-04T00:00:01.000Z",
        events: [...accepted("duplicate_claim").events],
      },
    });
    const claimed = await coordinator.claim({
      ownerId: "seed",
      attemptId: "seed_attempt",
      claimedAt: "2026-09-04T00:00:02.000Z",
      leaseTtlMs: 30_000,
    });
    expect(claimed.type).toBe("claimed");
    if (claimed.type !== "claimed") return;

    let claimCount = 0;
    const releases: Array<() => void> = [];
    runtime.run = async () => await new Promise<void>((resolve) => releases.push(resolve));
    const executor = worker("duplicate_worker", {
      maxConcurrent: 2,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        claim: async () => ++claimCount <= 2 ? claimed : { type: "empty" },
        settle: async () => ({ type: "settled", execution: claimed.execution }),
      },
    });

    await executor.poll();
    expect(releases).toHaveLength(2);
    releases[0]?.();
    await Promise.resolve();
    releases[1]?.();
    await executor.waitForIdle();
  });

  it("contains background polling errors when no observer is configured", async () => {
    let observedClaim: (() => void) | undefined;
    const claimStarted = new Promise<void>((resolve) => { observedClaim = resolve; });
    const executor = worker("no_observer", {
      pollIntervalMs: 2,
      onError: undefined,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        claim: async () => {
          observedClaim?.();
          throw new Error("unobserved poll failure");
        },
      },
    });

    executor.start();
    await claimStarted;
    await Promise.resolve();
    await Promise.resolve();
    executor.stop();
  });

  it("ignores empty and control-only batches and exposes admission conflicts", async () => {
    const executor = worker();
    await executor.sessionEventsAccepted({
      ...accepted("unused"),
      events: [],
    });
    await executor.sessionEventsAccepted({
      ...accepted("unused"),
      events: [{
        id: "system_01",
        type: "system.message",
        content: [{ type: "text", text: "metadata only" }],
        processedAt: "2026-09-04T00:00:01.000Z",
      }],
    });
    expect(runs).toEqual([]);

    await executor.sessionEventsAccepted(accepted("event_conflict"));
    await executor.waitForIdle();
    await expect(executor.sessionEventsAccepted({
      ...accepted("event_conflict"),
      events: [{
        ...accepted("event_conflict").events[0],
        content: [{ type: "text", text: "different payload" }],
      }],
    })).rejects.toThrow("conflicts with an existing admission");
  });

  it("settles missing context and runtime failures without stranding FIFO", async () => {
    const missing = worker("node_missing", {
      context: { find: async () => null },
    });
    await missing.sessionEventsAccepted(accepted("event_missing"));
    await missing.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_missing",
    })).resolves.toMatchObject({
      state: "failed",
      failure: "Session session_01 execution context was not found",
    });

    runtime.run = async () => { throw "string failure"; };
    const failing = worker("node_failure");
    await failing.sessionEventsAccepted(accepted("event_failure"));
    await failing.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_failure",
    })).resolves.toMatchObject({ state: "failed", failure: "string failure" });
  });

  it("cancels active work when the Session is stopped", async () => {
    let release: (() => void) | undefined;
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await new Promise<void>((resolve) => { release = resolve; });
    };
    const executor = worker();
    await executor.sessionEventsAccepted(accepted("event_stop"));
    await Promise.resolve();

    await executor.cancelSession({
      workspaceId: "workspace_01",
      sessionId: "session_01",
      reason: "Session archived",
    });
    expect(cancellations).toEqual(["session_stopped"]);
    release?.();
    await executor.waitForIdle();
    await expect(coordinator.find({
      workspaceId: "workspace_01",
      executionId: "event_stop",
    })).resolves.toMatchObject({ state: "cancelled" });
  });

  it("observes a cross-replica interrupt through a successful heartbeat", async () => {
    let release: (() => void) | undefined;
    runtime.run = async (input) => {
      runs.push(structuredClone(input));
      await new Promise<void>((resolve) => { release = resolve; });
    };
    const executor = worker("node_heartbeat", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async ({ fence }) => ({
          type: "renewed",
          fence: { ...fence, expiresAt: "2026-09-04T00:01:00.000Z" },
          interruptRequestedAt: "2026-09-04T00:00:03.000Z",
        }),
      },
    });
    await executor.sessionEventsAccepted(accepted("event_remote_interrupt"));
    await new Promise((resolve) => setTimeout(resolve, 15));

    expect(cancellations).toContain("interrupt_requested");
    release?.();
    await executor.waitForIdle();
  });

  it("refreshes the fence reference shared with a long-running runtime", async () => {
    let release: (() => void) | undefined;
    let observedFence: RunNodeSessionExecution["fence"] | undefined;
    runtime.run = async (input) => {
      observedFence = input.fence;
      await new Promise<void>((resolve) => { release = resolve; });
    };
    const executor = worker("node_fence_refresh", {
      heartbeatIntervalMs: 5,
      leaseTtlMs: 50,
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: coordinator.settle.bind(coordinator),
        renew: async ({ fence }) => ({
          type: "renewed" as const,
          fence: { ...fence, expiresAt: "2026-09-04T00:01:00.000Z" },
          interruptRequestedAt: null,
        }),
      },
    });
    await executor.sessionEventsAccepted(accepted("event_fence_refresh"));
    await new Promise((resolve) => setTimeout(resolve, 15));

    expect(observedFence?.expiresAt).toBe("2026-09-04T00:01:00.000Z");
    release?.();
    await executor.waitForIdle();
  });

  it("cancels a completed local attempt when settlement loses its fence", async () => {
    const executor = worker("node_settle_lost", {
      coordinator: {
        ...coordinator,
        admit: coordinator.admit.bind(coordinator),
        claim: coordinator.claim.bind(coordinator),
        renew: coordinator.renew.bind(coordinator),
        find: coordinator.find.bind(coordinator),
        requestInterrupt: coordinator.requestInterrupt.bind(coordinator),
        cancelSession: coordinator.cancelSession.bind(coordinator),
        settle: async () => ({ type: "lost" }),
      },
    });
    await executor.sessionEventsAccepted(accepted("event_settle_lost"));
    await executor.waitForIdle();

    expect(cancellations).toEqual(["lease_lost"]);
  });
});
