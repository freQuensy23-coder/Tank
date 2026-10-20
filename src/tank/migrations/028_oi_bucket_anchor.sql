-- Source-clock interpretation only: unknown historical anchors remain NULL.
-- No timestamp/price rewrite and no inferred START/END backfill.
ALTER TABLE mkt.oi_obs ADD COLUMN ts_bucket_anchor text;
ALTER TABLE mkt.oi_obs ADD CONSTRAINT valid_ts_bucket_anchor
  CHECK (ts_bucket_anchor IS NULL OR ts_bucket_anchor IN ('START','END','UNSPECIFIED'));
