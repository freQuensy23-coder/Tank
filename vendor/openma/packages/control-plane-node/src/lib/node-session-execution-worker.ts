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

export interface NodeSessionExecutionWorkerDependencies {
  coordinator: SessionExecutionCoordinatorPort;
  context: SessionExecutionContextSourcePort;
  runtime: NodeSessionExecutionRuntime;
  quiesceLostExecution?(input: {
    fence: Readonly<SessionExecutionFence>;
    context: SessionExecutionContext | null;
    currentFence?: Readonly<SessionExecutionFence>;
  }): Promise<void>;
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
  recovery: PendingRecovery | null;
  leaseLost: boolean;
  promise: Promise<void>;
}

interface PendingRecovery {
  priorFence: SessionExecutionFence;
  context: SessionExecutionContext | null;
  lostFence: SessionExecutionFence;
  attemptId: string;
}

/**
 * Durable Node execution scheduler. Admission, ordering and ownership live
 * behind the coordinator port. A single local slot tracks its drained attempt
 * until retained runtime quiescence and durable reconciliation are confirmed.
 */
export class NodeSessionExecutionWorker implements SessionEventDispatchPort {
  readonly #leaseTtlMs: number;
  readonly #heartbeatIntervalMs: number;
  readonly #pollIntervalMs: number;
  readonly #maxConcurrent: number;
  readonly #active = new Map<string, ActiveExecution>();
  #pendingRecovery: PendingRecovery | null = null;
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
      const active = [...this.#active.values()].map((entry) => entry.promise);
      if (active.length === 0) return;
      await Promise.allSettled(active);
    }
  }

  async #fillCapacity(): Promise<void> {
    while (!this.#stopped && this.#active.size < this.#maxConcurrent) {
      const pending = this.#pendingRecovery;
      const claimedAt = this.dependencies.clock.now().toISOString();
      const claimed = await this.dependencies.coordinator.claim({
        ownerId: this.dependencies.ownerId,
        attemptId: pending?.attemptId ?? this.dependencies.ids.nextAttemptId(),
        claimedAt,
        leaseTtlMs: this.#leaseTtlMs,
        ...(pending !== null && { recoverFrom: pending.priorFence }),
      });
      if (claimed.type === "empty") {
        if (pending !== null && await this.#reconcileEmptyRecovery(pending)) continue;
        return;
      }
      if (pending !== null) {
        const { fence } = claimed;
        if (fence.workspaceId !== pending.priorFence.workspaceId ||
          fence.sessionId !== pending.priorFence.sessionId ||
          fence.executionId !== pending.priorFence.executionId ||
          fence.ownerId !== this.dependencies.ownerId ||
          fence.attemptId !== pending.attemptId ||
          fence.generation !== pending.priorFence.generation + 1 ||
          claimed.execution.workspaceId !== fence.workspaceId ||
          claimed.execution.sessionId !== fence.sessionId ||
          claimed.execution.id !== fence.executionId) {
          throw new Error("Coordinator returned an unrelated execution during one-slot recovery");
        }
        this.#pendingRecovery = null;
      }
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
        recovery: pending,
        leaseLost: false,
        promise: Promise.resolve(),
      };
      this.#active.set(key, active);
      const fiber = runSupervised(
        Effect.tryPromise({
          try: () => this.#run(active),
          catch: (error: unknown) => error,
        }).pipe(
          Effect.ensuring(Effect.sync(() => {
            if (this.#active.get(key) === active) {
              if (this.#maxConcurrent === 1 && active.leaseLost) {
                this.#pendingRecovery = {
                  priorFence: { ...active.fence },
                  lostFence: active.recovery?.lostFence ?? { ...active.fence },
                  context: active.recovery === null ? active.context : active.recovery.context,
                  attemptId: this.dependencies.ids.nextAttemptId(),
                };
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
  }

  async #reconcileEmptyRecovery(pending: PendingRecovery): Promise<boolean> {
    const current = await this.dependencies.coordinator.find(pending.priorFence);
    switch (current?.state) {
      case "completed":
      case "failed":
      case "cancelled":
      case "queued":
        await this.#quiesce(pending);
        this.#pendingRecovery = null;
        return true;
      case "running": {
        const attempt = current.attempt;
        if (attempt?.id === pending.attemptId &&
          attempt.ownerId === this.dependencies.ownerId &&
          attempt.generation === pending.priorFence.generation + 1 &&
          Date.parse(attempt.leaseExpiresAt) <= this.dependencies.clock.now().getTime()) {
          pending.priorFence = {
            ...pending.priorFence,
            attemptId: attempt.id,
            generation: attempt.generation,
            expiresAt: attempt.leaseExpiresAt,
          };
          pending.attemptId = this.dependencies.ids.nextAttemptId();
        }
        return false;
      }
      default:
        throw new Error("Lost Session Execution disappeared during one-slot recovery");
    }
  }

  async #quiesce(pending: PendingRecovery, currentFence?: SessionExecutionFence): Promise<void> {
    if (this.dependencies.quiesceLostExecution === undefined) {
      throw new Error("One-slot recovery requires retained runtime quiescence");
    }
    await this.dependencies.quiesceLostExecution({
      fence: pending.lostFence,
      context: pending.context,
      ...(currentFence !== undefined && { currentFence }),
    });
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
    try {
      if (this.#maxConcurrent === 1 && this.#leaseDeadlinePassed(active)) {
        active.leaseLost = true;
        return;
      }
      if (active.recovery !== null) {
        try {
          await this.#quiesce(active.recovery, active.fence);
        } catch (error) {
          active.leaseLost = true;
          this.#reportError(error);
          return;
        }
        if (active.leaseLost || this.#leaseDeadlinePassed(active)) {
          active.leaseLost = true;
          return;
        }
        active.recovery = null;
        if (active.execution.interruptRequestedAt !== null) {
          active.cancellation ??= Promise.resolve();
        }
      }
      if (this.#maxConcurrent === 1 && active.cancellation !== null) {
        outcome = "cancelled";
      } else {
        phase = await this.#runRuntime(active);
        if (active.cancellation !== null) outcome = "cancelled";
      }
    }
    catch (error) {
      outcome = active.cancellation !== null ? "cancelled" : "failed";
      failure = executionFailure(error);
    }
    finally {
      active.drained = true;
      await interruptFiber(heartbeat);
      if (this.#maxConcurrent === 1) {
        try {
          await active.cancellation;
        } catch (error) {
          this.#reportError(error);
        }
      }
    }
    if (active.leaseLost)
      return;
    if (active.cancellation === null && outcome === 'completed' && phase?.yield !== undefined) {
      try {
        if (this.dependencies.coordinator.yieldProgress === undefined)
          throw new Error('Execution coordinator cannot confirm callback phase progress');
        const yielded = await this.dependencies.coordinator.yieldProgress({
          ...phase.yield, fence: active.fence,
          yieldedAt: this.dependencies.clock.now().toISOString()
        });
        if (yielded.type === 'lost') {
          active.leaseLost = true;
          await this.#cancel(active, 'lease_lost');
        }
      }
      catch (error) {
        if (this.#maxConcurrent === 1) active.leaseLost = true;
        this.#reportError(error);
      }
      return;
    }
    try {
      const settled = await this.dependencies.coordinator.settle({
        fence: active.fence,
        settledAt: this.dependencies.clock.now().toISOString(),
        outcome,
        ...(failure !== undefined && { failure }),
      });
      if (settled.type === "lost") {
        active.leaseLost = true;
        this.#logLeaseLost(active);
        await this.#cancel(active, "lease_lost");
      }
    }
    catch (error) {
      if (this.#maxConcurrent === 1) active.leaseLost = true;
      this.#reportError(error);
    }
  }

  async #runRuntime(active: ActiveExecution): Promise<void | SessionExecutionPhaseResult> {
    const context = await this.dependencies.context.find({
      workspaceId: active.execution.workspaceId,
      sessionId: active.execution.sessionId,
    });
    active.context = context;
    if (context === null) {
      throw new Error(`Session ${active.execution.sessionId} execution context was not found`);
    }
    if (active.leaseLost) return;
    if (this.#maxConcurrent === 1) {
      if (this.#leaseDeadlinePassed(active)) {
        active.leaseLost = true;
        return;
      }
      if (active.cancellation !== null) return;
    }
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
    if (this.#maxConcurrent === 1 && active.drained && reason !== "lease_lost") return;
    const cancellation = active.recovery !== null
      ? Promise.resolve()
      : Promise.resolve().then(() => this.dependencies.runtime.cancel({
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
