-- Ingestion can precede filtering: unknown relevance is SQL/JSON null, with an explicit reason.
ALTER TABLE ingest.source_items DROP CONSTRAINT IF EXISTS valid_relevance_decision;
UPDATE ingest.source_items SET relevance_decision=NULL,
 document=jsonb_set(jsonb_set(document,'{relevance,decision}','null'::jsonb),'{relevance,pending_reason}','"AWAITING_FILTER"'::jsonb)
 WHERE relevance_decision='PENDING';
ALTER TABLE ingest.source_items ADD CONSTRAINT valid_relevance_decision
 CHECK (relevance_decision IN ('ALWAYS','SEND','SKIP'));
