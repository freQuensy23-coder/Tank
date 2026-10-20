-- Wording belongs to the analyst skill (Technical Design §3.2 and §7.5).
-- Keep historic alert metadata; new alerts need no linter verdict.
ALTER TABLE out.alerts DROP CONSTRAINT IF EXISTS required_linter;
