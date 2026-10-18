-- OpenMA call IDs are unique within a session, not necessarily across sessions.
DROP INDEX IF EXISTS ops.ops_tool_calls_domain_id;
CREATE UNIQUE INDEX ops_tool_calls_session_call ON ops.tool_calls(session_id,call_id);
