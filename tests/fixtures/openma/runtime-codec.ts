// Exact executable excerpts from OpenMA85d5361 codec source SHA256 e2e01a8205d137a286c67e2d02f6440b0a3b8e7da7250354f92baea869e9adca.
export const OFFICIAL_RUNTIME_EVENT_TYPES = new Set([
  "user.message",
  "user.interrupt",
  "user.tool_confirmation",
  "user.custom_tool_result",
  "user.define_outcome",
  "user.tool_result",
  "system.message",
  "agent.custom_tool_use",
  "agent.mcp_tool_result",
  "agent.mcp_tool_use",
  "agent.message",
  "agent.thinking",
  "agent.thread_context_compacted",
  "agent.thread_message_received",
  "agent.thread_message_sent",
  "agent.tool_result",
  "agent.tool_use",
  "session.deleted",
  "session.error",
  "session.warning",
  "session.status_idle",
  "session.status_rescheduled",
  "session.status_running",
  "session.status_terminated",
  "session.thread_created",
  "session.thread_status_idle",
  "session.thread_status_rescheduled",
  "session.thread_status_running",
  "session.thread_status_terminated",
  "session.updated",
  "session.usage",
  "span.model_request_start",
  "span.model_request_end",
  "span.outcome_evaluation_start",
  "span.outcome_evaluation_ongoing",
  "span.outcome_evaluation_end",
]);

function camelCaseKey(key: string): string {
  return key.replace(/_([a-z0-9])/gu, (_match, value: string) =>
    value.toUpperCase(),
  );
}

function camelCaseRuntimeValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(camelCaseRuntimeValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      camelCaseKey(key),
      camelCaseRuntimeValue(item),
    ]),
  );
}

function snakeCaseKey(key: string): string {
  return key.replace(/[A-Z]/gu, (value) => `_${value.toLowerCase()}`);
}

function snakeCaseRuntimeValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(snakeCaseRuntimeValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      snakeCaseKey(key),
      snakeCaseRuntimeValue(item),
    ]),
  );
}

export function encodeRuntimeHistoryEvent(event: SessionEventView): object {
  return snakeCaseRuntimeValue(event) as object;
}

export function decodeRuntimeEvent(
  value: unknown,
  deltaTypes: ReadonlySet<SessionEventDeltaType>,
): StreamSessionEvent[] {
  if (value === null || typeof value !== "object") return [];
  const raw = value as Record<string, unknown>;
  if (typeof raw.type !== "string") return [];
  if (
    raw.type === "agent.message_stream_start" ||
    raw.type === "agent.thinking_stream_start"
  ) {
    const type = raw.type.startsWith("agent.message")
      ? "agent.message"
      : "agent.thinking";
    const id = type === "agent.message" ? raw.message_id : raw.thinking_id;
    return deltaTypes.has(type) && typeof id === "string"
      ? [{ type: "event_start", event: { id, type } }]
      : [];
  }
  if (raw.type === "agent.message_chunk" || raw.type === "agent.thinking_chunk") {
    const type = raw.type === "agent.message_chunk"
      ? "agent.message"
      : "agent.thinking";
    const id = type === "agent.message" ? raw.message_id : raw.thinking_id;
    return deltaTypes.has(type) &&
      typeof id === "string" &&
      typeof raw.delta === "string"
      ? [
          {
            type: "event_delta",
            eventId: id,
            delta: {
              type: "content_delta",
              content: { type: "text", text: raw.delta },
              ...(typeof raw.index === "number" && { index: raw.index }),
            },
          },
        ]
      : [];
  }
  if (
    raw.type === "agent.message_stream_end" ||
    raw.type === "agent.thinking_stream_end"
  ) return [];
  if (!OFFICIAL_RUNTIME_EVENT_TYPES.has(raw.type)) return [];

  const decoded = camelCaseRuntimeValue(raw) as Record<string, unknown>;
  // event_start/event_delta use the runtime's stable block ids. Preserve the
  // same id on the eventual committed event so SDK consumers can atomically
  // replace their in-flight projection instead of guessing from arrival order.
  if (raw.type === "agent.message" && typeof raw.message_id === "string") {
    decoded.id = raw.message_id;
  }
  if (raw.type === "agent.thinking" && typeof raw.thinking_id === "string") {
    decoded.id = raw.thinking_id;
  }
  if (raw.type === "session.error") {
    decoded.error = normalizeRuntimeError(raw.error);
  }
  if (raw.type === "span.model_request_end") {
    decoded.modelUsage = normalizeModelUsage(raw.model_usage ?? raw.modelUsage);
    decoded.isError =
      typeof raw.is_error === "boolean"
        ? raw.is_error
        : typeof raw.isError === "boolean"
          ? raw.isError
          : null;
  }
  if (raw.type === "span.outcome_evaluation_end") {
    decoded.usage = normalizeModelUsage(raw.usage);
  }
  if (
    (raw.type === "agent.tool_result" || raw.type === "agent.mcp_tool_result")
    && typeof raw.content === "string"
  ) {
    // The legacy harness wire emits scalar tool output while the official
    // Managed Agents history contract requires an array of content blocks.
    // Normalize at the runtime boundary so every store/transport sees the
    // same canonical application shape.
    decoded.content = [{ type: "text", text: raw.content }];
  }
  return [decoded as unknown as StreamSessionEvent];
}
