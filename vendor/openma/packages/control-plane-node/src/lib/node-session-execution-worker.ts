import type { SessionExecutionPhaseResult } from '@open-managed-agents/session-runtime-contract/coordination';
import type {
  AcceptedSessionEvents,
  SessionEventDispatchPort,
} from "@open-managed-agents/session-runtime-contract/dispatch";
import type { SessionExecution, SessionExecutionCoordinatorPort, SessionExecutionFence, } from "@open-managed-agents/session-runtime-contract/coordination";
import {
  sessionExecutionEventBatches,
  sessionExecutionEvents,
} from "@open-managed-agents/session-runtime-contract/coordination";
import type {
  SessionExecutionContext,
  SessionExecutionContextSourcePort,
} from "@open-managed-agents/session-runtime-contract/context";
import {
  emitDbBoundaryLog,
  withDbBoundary,
} from "@open-managed-agents/sql-client";
import { executionFailure } from "./execution-recovery-refusal.js";
import { Effect, Fiber } from "effect";
import { interruptFiber, runSupervised } from "./supervised-fiber.js";
import { RetainedExecutionFenceLost, RetainedIdlePendingTimeout, RetainedIdleUnconfirmed } from "./tank-retained/idle-errors.js";

export interface RunNodeSessionExecution extends AcceptedSessionEvents {
  executionId: string;
  fence: SessionExecutionFence;
}

export interface CancelNodeSessionExecution {
  workspaceId: string;
  sessionId: string;
  executionId: string;
  fence: SessionExecutionFence;
  reason: "interrupt_requested" | "lease_lost" | "session_stopped";
}

export interface NodeSessionExecutionRuntime {
  run(input: RunNodeSessionExecution): Promise<void | SessionExecutionPhaseResult>;
  cancel(input: CancelNodeSessionExecution): Promise<void>;
}

export interface SessionClaimScope {
  workspaceId: string;
  sessionId: string;
}

export interface StartupResident extends SessionClaimScope {
  quiesce(): Promise<void>;
}

export interface NodeSessionExecutionWorkerDependencies {
  coordinator: SessionExecutionCoordinatorPort;
  context: SessionExecutionContextSourcePort;
  runtime: NodeSessionExecutionRuntime;
  quiesceLostExecution?(input: {
    fence: Readonly<SessionExecutionFence>;
    context: SessionExecutionContext | null;
  }): Promise<void>;
  discoverStartupResidents?(): Promise<readonly StartupResident[]>;
  listClaimScopes?(now: Date): Promise<readonly SessionClaimScope[]>;
  ownerId: string;
  clock: { now(): Date };
  ids: { nextAttemptId(): string };
  leaseTtlMs?: number;
  heartbeatIntervalMs?: number;
  pollIntervalMs?: number;
  maxConcurrent?: number;
  onError?(error: Error): void;
}

interface ActiveExecution {
  execution: SessionExecution;
  fence: SessionExecutionFence;
  context: SessionExecutionContext | null;
  cancellation: Promise<void> | null;
  drained: boolean;
  leaseLost: boolean;
  quarantined: "until_expiry" | "indefinite" | null;
  promise: Promise<void>;
}

interface PendingRecovery {
  priorFence: SessionExecutionFence;
  context: SessionExecutionContext | null;
  quiesced: boolean;
  retryAt: number;
  attemptId: string;
}

export class NodeSessionExecutionWorker implements SessionEventDispatchPort {
  readonly #leaseTtlMs: number;
  readonly #heartbeatIntervalMs: number;
  readonly #pollIntervalMs: number;
  readonly #maxConcurrent: number;
  readonly #active = new Map<string, ActiveExecution>();
  readonly #pendingRecovery = new Set<PendingRecovery>();
  #startupResidents: Map<StartupResident, number> | null = null;
  readonly #quiescing = new Map<StartupResident | PendingRecovery, Promise<void>>();
  #polling: Promise<void> | null = null;
  #pollFiber: Fiber.RuntimeFiber<void, never> | null = null;
  #stopped = false;

  constructor(
    private readonly dependencies: NodeSessionExecutionWorkerDependencies,
  ) {
    this.#leaseTtlMs = dependencies.leaseTtlMs ?? 30_000;
    this.#heartbeatIntervalMs = dependencies.heartbeatIntervalMs ?? 10_000;
    this.#pollIntervalMs = dependencies.pollIntervalMs ?? 1_000;
    this.#maxConcurrent = dependencies.maxConcurrent ?? 8;
    if (this.#heartbeatIntervalMs >= this.#leaseTtlMs) {
      throw new Error("Session Execution heartbeat must be shorter than its lease");
    }
    if (this.#maxConcurrent < 1 || !Number.isInteger(this.#maxConcurrent)) {
      throw new Error("Session Execution maxConcurrent must be a positive integer");
    }
  }

  start(): void {
    if (this.#pollFiber !== null) return;
    this.#stopped = false;
    this.#pollFiber = runSupervised(
      Effect.gen(this, function* () {
        while (true) {
          yield* Effect.tryPromise({
            try: () => this.poll(),
            catch: (error: unknown) => error,
          }).pipe(Effect.catchAll((error) => Effect.sync(() => {
            this.#reportError(error);
          })));
          yield* Effect.sleep(this.#pollIntervalMs);
        }
      }),
      (error) => this.#reportError(error),
    );
  }

  stop(): void {
    this.#stopped = true;
    const fiber = this.#pollFiber;
    if (fiber === null) return;
    this.#pollFiber = null;
    void interruptFiber(fiber);
  }

  async sessionEventsAccepted(input: AcceptedSessionEvents): Promise<void> {
    if (input.events.length === 0) return;
    for (const interrupt of input.events.filter(
      (event) => event.type === "user.interrupt",
    )) {
      const requestedAt = interrupt.processedAt ??
        this.dependencies.clock.now().toISOString();
      const laneId = interrupt.sessionThreadId ?? undefined;
      await this.dependencies.coordinator.requestInterrupt({
        workspaceId: input.workspaceId,
        sessionId: input.sessionId,
        ...(laneId !== undefined && { laneId }),
        requestedAt,
      });
      for (const active of this.#active.values()) {
        if (
          active.execution.workspaceId === input.workspaceId &&
          active.execution.sessionId === input.sessionId &&
          (laneId === undefined || active.execution.laneId === laneId)
        ) {
          await this.#cancel(active, "interrupt_requested");
        }
      }
    }

    const executionEvents = sessionExecutionEvents(input.events);
    if (executionEvents.length === 0) return;
    for (const batch of sessionExecutionEventBatches(input.events)) {
      const admitted = await this.dependencies.coordinator.admit({
        execution: {
          id: batch.id,
          workspaceId: input.workspaceId,
          sessionId: input.sessionId,
          laneId: batch.laneId,
          admittedAt: batch.events.find((event) => event.type !== "system.message")
            ?.processedAt ??
            this.dependencies.clock.now().toISOString(),
          events: batch.events,
        },
      });
      if (admitted.type === "conflict") {
        throw new Error(
          `Session Execution ${batch.id} conflicts with an existing admission`,
        );
      }
    }
    await this.poll();
  }

  async cancelSession(input: {
    workspaceId: string;
    sessionId: string;
    reason: string;
  }): Promise<void> {
    await this.dependencies.coordinator.cancelSession({
      ...input,
      cancelledAt: this.dependencies.clock.now().toISOString(),
    });
    for (const active of this.#active.values()) {
      if (
        active.execution.workspaceId === input.workspaceId &&
        active.execution.sessionId === input.sessionId
      ) {
        await this.#cancel(active, "session_stopped");
      }
    }
  }

  poll(): Promise<void> {
    if (this.#polling !== null) return this.#polling;
    if (this.#stopped) return Promise.resolve();
    const polling = this.#fillCapacity();
    this.#polling = polling;
    const clear = () => {
      this.#polling = null;
    };
    // `finally()` would create a second rejected Promise when polling fails.
    // Observe both branches explicitly so the background caller owns the only
    // rejection and can report it through onError.
    void polling.then(clear, clear);
    return polling;
  }

  async waitForIdle(): Promise<void> {
    while (true) {
      await this.poll();
      const active = [...this.#active.values()].filter((entry) => !entry.quarantined).map((entry) => entry.promise);
      if (active.length === 0) {
        if (this.#stopped) await Promise.allSettled(this.#quiescing.values());
        return;
      }
      await Promise.allSettled(active);
    }
  }

  async #fillCapacity(): Promise<void> {
    this.#startupResidents ??= new Map(
      (await this.dependencies.discoverStartupResidents?.() ?? []).map((resident) => [resident, 0]),
    );
    if (this.#stopped) return;
    for (const [key, active] of this.#active) {
      if (active.quarantined !== "until_expiry" || !this.#leaseDeadlinePassed(active)) continue;
      this.#logLeaseLost(active);
      this.#enqueueRecovery(active);
      this.#active.delete(key);
    }
    const startup = this.#startupResidents;
    const now = performance.now();
    for (const [resident, retryAt] of startup) {
      if (retryAt > now) continue;
      this.#checkQuiescence(resident, async () => {
        try {
          await resident.quiesce();
          startup.delete(resident);
        } catch (error) {
          startup.set(resident, performance.now() + this.#pollIntervalMs);
          throw error;
        }
      });
    }
    for (const pending of this.#pendingRecovery) {
      if (pending.quiesced || pending.retryAt > now) continue;
      this.#checkQuiescence(pending, async () => {
        await Promise.all([
          this.#sweepWithoutClaim(pending.priorFence).catch((error: unknown) => this.#reportError(error)),
          this.#quiescePending(pending),
        ]);
      });
    }
    if (this.#maxConcurrent === 1) await Promise.all(this.#quiescing.values());

    const ready = [...this.#pendingRecovery].filter(
      (pending) => pending.quiesced && !this.#quiescing.has(pending) && pending.retryAt <= performance.now(),
    );
    for (const pending of ready) {
      if (this.#stopped) return;
      if (this.#occupiedSlots() >= this.#maxConcurrent
        || [...startup.keys()].some((resident) => resident.workspaceId === pending.priorFence.workspaceId
          && resident.sessionId === pending.priorFence.sessionId)) continue;
      try {
        const claimed = await this.#claim(pending).catch((error: unknown) => {
          this.#reportError(error);
          return null;
        });
        if (claimed?.type === "claimed") {
          this.#pendingRecovery.delete(pending);
          this.#launch(claimed);
        } else {
          await this.#reconcileEmptyRecovery(pending);
        }
      } catch (error) {
        this.#reportError(error);
      }
      pending.retryAt = performance.now() + this.#pollIntervalMs;
    }

    await this.#claimAvailableWork();
  }

  #checkQuiescence(key: StartupResident | PendingRecovery, check: () => Promise<void>): void {
    if (this.#quiescing.has(key)) return;
    const task = check().catch((error: unknown) => this.#reportError(error)).finally(() => {
      this.#quiescing.delete(key);
    });
    this.#quiescing.set(key, task);
  }

  async #quiescePending(pending: PendingRecovery): Promise<void> {
    try {
      if (this.dependencies.quiesceLostExecution === undefined) {
        throw new Error("Recovery requires retained runtime quiescence");
      }
      await this.dependencies.quiesceLostExecution({
        fence: pending.priorFence,
        context: pending.context,
      });
      pending.quiesced = true;
    } catch (error) {
      pending.retryAt = performance.now() + this.#pollIntervalMs;
      this.#reportError(error);
    }
  }

  #blockedScopes(): SessionClaimScope[] {
    return [
      ...(this.#startupResidents?.keys() ?? []),
      ...[...this.#pendingRecovery].map((pending) => pending.priorFence),
    ];
  }

  async #claimAvailableWork(): Promise<void> {
    if (this.#stopped || this.#occupiedSlots() >= this.#maxConcurrent) return;
    const blocked = this.#blockedScopes();
    if (this.#maxConcurrent === 1 && blocked.length > 0) return;

    if (blocked.length === 0 && this.#active.size === 0) {
      const claimed = await this.#claim();
      if (claimed.type === "empty") return;
      this.#launch(claimed);
    }
    if (this.#stopped || this.#occupiedSlots() >= this.#maxConcurrent) return;

    if (this.dependencies.listClaimScopes === undefined) {
      while (!this.#stopped && this.#occupiedSlots() < this.#maxConcurrent && this.#blockedScopes().length === 0) {
        const claimed = await this.#claim();
        if (claimed.type === "empty") return;
        this.#launch(claimed);
      }
      return;
    }

    for (const scope of await this.dependencies.listClaimScopes(this.dependencies.clock.now())) {
      if (this.#stopped || this.#occupiedSlots() >= this.#maxConcurrent) return;
      const occupied = [...this.#blockedScopes(), ...[...this.#active.values()].map((active) => active.fence)];
      if (occupied.some((entry) => entry.workspaceId === scope.workspaceId && entry.sessionId === scope.sessionId)) continue;
      const claimed = await this.#claim(undefined, scope);
      if (claimed.type === "claimed") this.#launch(claimed);
    }
  }

  #occupiedSlots(): number {
    return this.#active.size + (this.#startupResidents?.size ?? 0)
      + [...this.#pendingRecovery].filter((pending) => !pending.quiesced).length;
  }

  async #sweepWithoutClaim(fence: SessionExecutionFence): Promise<void> {
    const result = await this.dependencies.coordinator.claim({
      ownerId: `${fence.ownerId}:sweep`,
      attemptId: this.dependencies.ids.nextAttemptId(),
      claimedAt: this.dependencies.clock.now().toISOString(),
      leaseTtlMs: this.#leaseTtlMs,
      recoverFrom: fence,
    });
    if (result.type !== "empty") throw new Error("Coordinator claimed an execution during an expiry sweep");
  }

  async #claim(pending?: PendingRecovery, scope?: SessionClaimScope) {
    const claim = () => this.dependencies.coordinator.claim({
      ...scope,
      ownerId: this.dependencies.ownerId,
      attemptId: pending?.attemptId ?? this.dependencies.ids.nextAttemptId(),
      claimedAt: this.dependencies.clock.now().toISOString(),
      leaseTtlMs: this.#leaseTtlMs,
      ...(pending !== undefined && { recoverFrom: pending.priorFence }),
    });
    const claimed = pending === undefined ? await claim() : await withDbBoundary({
      op: "session_execution.recovery_claim",
      sessionId: pending.priorFence.sessionId,
      timeoutMs: this.#leaseTtlMs,
    }, claim);
    if (pending !== undefined && claimed.type === "claimed") {
      const { fence, execution } = claimed;
      if (fence.workspaceId !== pending.priorFence.workspaceId ||
        fence.sessionId !== pending.priorFence.sessionId ||
        fence.executionId !== pending.priorFence.executionId ||
        fence.ownerId !== this.dependencies.ownerId ||
        fence.attemptId !== pending.attemptId ||
        fence.generation !== pending.priorFence.generation + 1 ||
        execution.workspaceId !== fence.workspaceId ||
        execution.sessionId !== fence.sessionId ||
        execution.id !== fence.executionId) {
        throw new Error("Coordinator returned an unrelated execution during recovery");
      }
    }
    return claimed;
  }

  #enqueueRecovery(active: ActiveExecution): void {
    this.#pendingRecovery.add({
      priorFence: { ...active.fence },
      context: active.context,
      quiesced: active.context === null,
      retryAt: 0,
      attemptId: this.dependencies.ids.nextAttemptId(),
    });
  }

  #launch(claimed: { execution: SessionExecution; fence: SessionExecutionFence }): void {
    const key = JSON.stringify([
      claimed.execution.workspaceId,
      claimed.execution.id,
    ]);
    const active: ActiveExecution = {
      execution: claimed.execution,
      fence: claimed.fence,
      context: null,
      cancellation: null,
      drained: false,
      leaseLost: false,
      quarantined: null,
      promise: Promise.resolve(),
    };
    this.#active.set(key, active);
    const fiber = runSupervised(
      Effect.tryPromise({
        try: () => this.#run(active),
        catch: (error: unknown) => error,
      }).pipe(
        Effect.ensuring(Effect.sync(() => {
          if (this.#active.get(key) === active && !active.quarantined) {
            if (active.leaseLost && this.#tracksRecovery()) {
              this.#enqueueRecovery(active);
            }
            this.#active.delete(key);
          }
          this.#pollInBackground();
        })),
        Effect.catchAll((error) => Effect.sync(() => {
          this.#reportError(error);
        })),
      ),
      (error) => this.#reportError(error),
    );
    active.promise = Effect.runPromise(Fiber.join(fiber)).then(
      () => undefined,
      (error: unknown) => {
        this.#reportError(error);
      },
    );
  }

  async #reconcileEmptyRecovery(pending: PendingRecovery): Promise<void> {
    const current = await withDbBoundary({
      op: "session_execution.recovery_find",
      sessionId: pending.priorFence.sessionId,
      timeoutMs: this.#leaseTtlMs,
    }, () => this.dependencies.coordinator.find(pending.priorFence));
    const attempt = current?.state === "running" ? current.attempt : null;
    if (!attempt) {
      this.#pendingRecovery.delete(pending);
      return;
    }
    if (attempt.ownerId === this.dependencies.ownerId &&
      attempt.id === pending.priorFence.attemptId &&
      attempt.generation === pending.priorFence.generation) return;
    if (attempt.ownerId === this.dependencies.ownerId &&
      attempt.id === pending.attemptId &&
      attempt.generation === pending.priorFence.generation + 1) {
      if (Date.parse(attempt.leaseExpiresAt) > this.dependencies.clock.now().getTime()) return;
      pending.priorFence = {
        ...pending.priorFence,
        attemptId: attempt.id,
        generation: attempt.generation,
        expiresAt: attempt.leaseExpiresAt,
      };
      pending.attemptId = this.dependencies.ids.nextAttemptId();
      return;
    }
    this.#pendingRecovery.delete(pending);
  }

  async #run(active: ActiveExecution): Promise<void> {
    const heartbeat = runSupervised(Effect.gen(this, function* () {
      while (!active.leaseLost) {
        yield* Effect.sleep(this.#heartbeatIntervalMs);
        if (active.leaseLost)
          return;
        yield* Effect.tryPromise({
          try: () => this.#heartbeat(active),
          catch: (error: unknown) => error,
        }).pipe(Effect.catchAll((error) => Effect.tryPromise({
          try: () => this.#loseLease(active, error),
          catch: (cancelError: unknown) => cancelError,
        }).pipe(Effect.catchAll((cancelError) => Effect.sync(() => {
          this.#reportError(cancelError);
        })))));
      }
    }), (error) => this.#reportError(error));
    let outcome: "completed" | "failed" | "cancelled" = "completed";
    let failure: string | undefined;
    let phase: void | SessionExecutionPhaseResult = undefined;
    let idleFailure: RetainedIdleUnconfirmed | undefined;
    try {
      if (this.#leaseDeadlinePassed(active)) {
        active.leaseLost = true;
        return;
      }
      if (active.execution.interruptRequestedAt !== null &&
        this.dependencies.quiesceLostExecution !== undefined) {
        active.cancellation ??= Promise.resolve();
      }
      if (active.cancellation !== null) {
        outcome = "cancelled";
      } else {
        phase = await this.#runRuntime(active);
        if (active.cancellation !== null) outcome = "cancelled";
      }
    }
    catch (error) {
      if (error instanceof RetainedIdleUnconfirmed) {
        idleFailure = error;
        this.#reportError(error);
      } else if (error instanceof RetainedExecutionFenceLost) {
        active.leaseLost = true;
        this.#logLeaseLost(active);
        return;
      } else {
        outcome = active.cancellation !== null ? "cancelled" : "failed";
        failure = executionFailure(error);
      }
    }
    finally {
      active.drained = true;
      await interruptFiber(heartbeat);
      if (this.#tracksRecovery()) {
        try {
          await active.cancellation;
        } catch (error) {
          this.#reportError(error);
        }
      }
    }
    if (idleFailure !== undefined) {
      if (this.#tracksRecovery() && (active.leaseLost || this.#leaseDeadlinePassed(active))) {
        if (!active.leaseLost) this.#logLeaseLost(active);
        active.leaseLost = true;
      } else {
        active.quarantined = this.#tracksRecovery() && idleFailure instanceof RetainedIdlePendingTimeout
          ? "until_expiry" : "indefinite";
      }
      return;
    }
    if (active.leaseLost)
      return;
    if (active.cancellation === null && outcome === 'completed' && phase?.yield !== undefined) {
      try {
        const progress = phase.yield;
        const coordinator = this.dependencies.coordinator;
        const yielded = await withDbBoundary({
          op: "session_execution.yield_ack",
          sessionId: active.fence.sessionId,
          timeoutMs: this.#leaseTtlMs,
        }, () => {
          if (coordinator.yieldProgress === undefined)
            throw new Error("Execution coordinator cannot confirm callback phase progress");
          return coordinator.yieldProgress({
            ...progress, fence: active.fence,
            yieldedAt: this.dependencies.clock.now().toISOString(),
          });
        });
        if (yielded.type === 'lost') {
          active.leaseLost = true;
          await this.#cancel(active, 'lease_lost');
        }
      }
      catch (error) {
        if (this.#tracksRecovery()) active.leaseLost = true;
        this.#reportError(error);
      }
      return;
    }
    try {
      const settled = await withDbBoundary({
        op: "session_execution.settle_ack",
        sessionId: active.fence.sessionId,
        timeoutMs: this.#leaseTtlMs,
      }, () => this.dependencies.coordinator.settle({
        fence: active.fence,
        settledAt: this.dependencies.clock.now().toISOString(),
        outcome,
        ...(failure !== undefined && { failure }),
      }));
      if (settled.type === "lost") {
        active.leaseLost = true;
        this.#logLeaseLost(active);
        await this.#cancel(active, "lease_lost");
      }
    }
    catch (error) {
      if (this.#tracksRecovery()) active.leaseLost = true;
      this.#reportError(error);
    }
  }

  async #runRuntime(active: ActiveExecution): Promise<void | SessionExecutionPhaseResult> {
    const context = await this.dependencies.context.find({
      workspaceId: active.execution.workspaceId,
      sessionId: active.execution.sessionId,
    });
    if (context === null) {
      throw new Error(`Session ${active.execution.sessionId} execution context was not found`);
    }
    if (active.leaseLost) return;
    if (this.#leaseDeadlinePassed(active)) {
      active.leaseLost = true;
      return;
    }
    if (active.cancellation !== null) return;
    active.context = context;
    return this.dependencies.runtime.run({
      executionId: active.execution.id,
      workspaceId: active.execution.workspaceId,
      sessionId: active.execution.sessionId,
      session: context.session,
      environment: context.environment,
      events: active.execution.events,
      fence: active.fence,
    });
  }

  async #heartbeat(active: ActiveExecution): Promise<void> {
    if (active.leaseLost) return;
    let renewed: Awaited<ReturnType<NodeSessionExecutionWorkerDependencies["coordinator"]["renew"]>>;
    try {
      renewed = await withDbBoundary({
        op: "session_execution.heartbeat",
        sessionId: active.execution.sessionId,
        timeoutMs: this.#heartbeatIntervalMs,
      }, () => this.dependencies.coordinator.renew({
        fence: active.fence,
        renewedAt: this.dependencies.clock.now().toISOString(),
        leaseTtlMs: this.#leaseTtlMs,
      }));
    } catch (error) {
      if (active.drained) return;
      if (this.#leaseDeadlinePassed(active)) {
        await this.#loseLease(active, error);
      } else {
        this.#reportError(error);
      }
      return;
    }
    if (active.drained) return;
    if (renewed.type === "lost") {
      active.leaseLost = true;
      this.#logLeaseLost(active);
      await this.#cancel(active, "lease_lost");
      return;
    }
    // The runtime driver keeps the fence reference for the whole stream.
    // Refresh that reference in place so output emitted after a heartbeat is
    // committed with the current lease generation/expiry.
    Object.assign(active.fence, renewed.fence);
    if (renewed.interruptRequestedAt !== null) {
      await this.#cancel(active, "interrupt_requested");
    }
  }

  #tracksRecovery(): boolean {
    return this.#maxConcurrent === 1 || this.dependencies.quiesceLostExecution !== undefined;
  }

  #leaseDeadlinePassed(active: ActiveExecution): boolean {
    return Date.parse(active.fence.expiresAt) <= this.dependencies.clock.now().getTime();
  }

  #logLeaseLost(active: ActiveExecution): void {
    emitDbBoundaryLog({
      op: "session_execution.lease",
      sessionId: active.execution.sessionId,
      durationMs: 0,
      retryCount: 0,
      outcome: "error",
      errorTag: "LeaseLost",
    });
  }

  async #loseLease(active: ActiveExecution, error: unknown): Promise<void> {
    this.#reportError(error);
    // A thrown renew/heartbeat error is not a confirmed CAS miss. Keep the
    // turn until the lease deadline actually passes.
    if (active.drained || !this.#leaseDeadlinePassed(active)) return;
    active.leaseLost = true;
    try {
      await this.#cancel(active, "lease_lost");
    } catch (cancelError) {
      this.#reportError(cancelError);
    }
  }

  #pollInBackground(): void {
    const polling = this.poll();
    runSupervised(
      Effect.tryPromise({
        try: () => polling,
        catch: (error: unknown) => error,
      }).pipe(Effect.catchAll((error) => Effect.sync(() => {
        this.#reportError(error);
      }))),
      (error) => this.#reportError(error),
    );
  }

  #reportError(error: unknown): void {
    try {
      this.dependencies.onError?.(
        error instanceof Error ? error : new Error(String(error)),
      );
    } catch {
      // Observability hooks must never kill the durable scheduler.
    }
  }

  async #cancel(
    active: ActiveExecution,
    reason: CancelNodeSessionExecution["reason"],
  ): Promise<void> {
    if (active.cancellation !== null) return active.cancellation;
    if (active.drained && reason !== "lease_lost" && this.#tracksRecovery()) return;
    const cancellation = Promise.resolve().then(() => this.dependencies.runtime.cancel({
      workspaceId: active.execution.workspaceId,
      sessionId: active.execution.sessionId,
      executionId: active.execution.id,
      fence: active.fence,
      reason,
    }));
    active.cancellation = cancellation;
    return cancellation;
  }
}
