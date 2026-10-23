ALTER TABLE out.ops_notices DROP CONSTRAINT current_notice_kind;
ALTER TABLE out.ops_notices ADD CONSTRAINT current_notice_kind
    CHECK (kind IN ('DELIVERY_FAILED', 'HEARTBEAT', 'ITEM_PROGRESS')) NOT VALID;
