-- Subscription-funded sessions have no daily spend cap. Preserve prior state as historical audit.
CREATE SCHEMA IF NOT EXISTS archive;
ALTER TABLE ops.budget_state SET SCHEMA archive;
ALTER TABLE archive.budget_state RENAME TO ops_budget_state;
CREATE TRIGGER archived_budget_readonly BEFORE UPDATE OR DELETE ON archive.ops_budget_state
 FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
-- NOT VALID preserves historical LLM_BUDGET notices while enforcing the current enum on new rows.
ALTER TABLE out.ops_notices ADD CONSTRAINT current_notice_kind
 CHECK(kind IN ('DELIVERY_FAILED','HEARTBEAT')) NOT VALID;
