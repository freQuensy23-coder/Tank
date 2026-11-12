export interface SseRelayStream {
  onAbort(listener: () => void): void;
}

export interface SseRelayHandlers<T> {
  heartbeat(): Promise<unknown>;
  deliver(event: T): Promise<boolean>;
}

/**
 * Relays an event source to one SSE connection until the source ends, the
 * connection drops or a delivery asks to stop. The pending next() gets one
 * continuation per event, never one per heartbeat: a Session that stays idle
 * would otherwise add a reaction to the same pending promise every interval,
 * for as long as the connection lives. The source is always returned so its
 * subscription is released when the client disconnects.
 */
export async function relayWithHeartbeat<T>(
  stream: SseRelayStream,
  events: AsyncIterable<T>,
  heartbeatIntervalMs: number,
  handlers: SseRelayHandlers<T>,
): Promise<void> {
  const iterator = events[Symbol.asyncIterator]();
  const aborted = new Promise<{ type: "aborted" }>((resolve) => {
    stream.onAbort(() => resolve({ type: "aborted" }));
  });
  const pull = () =>
    iterator.next().then((event) => ({ type: "event" as const, event }));
  let pendingEvent = pull();
  try {
    for (;;) {
      let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
      const next = await Promise.race([
        pendingEvent,
        aborted,
        new Promise<{ type: "heartbeat" }>((resolve) => {
          heartbeatTimer = setTimeout(
            () => resolve({ type: "heartbeat" }),
            heartbeatIntervalMs,
          );
        }),
      ]);
      if (heartbeatTimer !== undefined) clearTimeout(heartbeatTimer);
      if (next.type === "aborted") return;
      if (next.type === "heartbeat") {
        await handlers.heartbeat();
        continue;
      }
      if (next.event.done) return;
      if (!(await handlers.deliver(next.event.value))) return;
      pendingEvent = pull();
    }
  } finally {
    void Promise.resolve(iterator.return?.()).catch(() => undefined);
  }
}
