export interface SseRelayStream {
  onAbort(listener: () => void): void;
}

export interface SseRelayHandlers<T> {
  heartbeat(): Promise<unknown>;
  deliver(event: T): Promise<boolean>;
}

/**
 * Relays an event source to one SSE connection until the source ends, the
 * connection drops or a delivery asks to stop. Nothing accumulates while the
 * connection stays open: the abort listener is registered once, every pending
 * next() gets exactly one continuation, and the heartbeat timer is re-armed
 * for each wait. The loop sleeps on one resolver that is replaced per wait,
 * so no promise ever collects a reaction per heartbeat or per event. The
 * source is returned exactly once so its subscription is released when the
 * client disconnects.
 */
export async function relayWithHeartbeat<T>(
  stream: SseRelayStream,
  events: AsyncIterable<T>,
  heartbeatIntervalMs: number,
  handlers: SseRelayHandlers<T>,
): Promise<void> {
  const iterator = events[Symbol.asyncIterator]();
  let wake: (() => void) | undefined;
  let aborted = false;
  let arrived: IteratorResult<T> | undefined;
  let failure: { error: unknown } | undefined;
  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  const notify = () => {
    const resolve = wake;
    wake = undefined;
    resolve?.();
  };
  const pull = () => {
    iterator.next().then(
      (result) => {
        arrived = result;
        notify();
      },
      (error) => {
        failure = { error };
        notify();
      },
    );
  };
  stream.onAbort(() => {
    aborted = true;
    notify();
  });
  try {
    pull();
    for (;;) {
      if (!aborted && !arrived && !failure) {
        heartbeatTimer = setTimeout(notify, heartbeatIntervalMs);
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
        clearTimeout(heartbeatTimer);
        heartbeatTimer = undefined;
      }
      if (aborted) return;
      if (failure) throw failure.error;
      if (arrived) {
        const result = arrived;
        arrived = undefined;
        if (result.done) return;
        if (!(await handlers.deliver(result.value))) return;
        pull();
        continue;
      }
      await handlers.heartbeat();
    }
  } finally {
    if (heartbeatTimer !== undefined) clearTimeout(heartbeatTimer);
    void Promise.resolve(iterator.return?.()).catch(() => undefined);
  }
}
