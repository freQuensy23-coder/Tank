-- Wording belongs to the analyst skill. Historical frozen JSON documents and
-- evidence bundles remain unchanged; the retired empty projection is removed.
ALTER TABLE out.alerts DROP COLUMN IF EXISTS linter;
