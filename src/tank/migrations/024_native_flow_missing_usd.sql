-- §0: missing conversion data is null with a reason, never an invented USD zero.
-- Native quantities and trade coverage remain usable without historical quote/USD FX.
-- Preserve old evidence; enforce the reason contract on all new/updated rows.
ALTER TABLE mkt.flow_bars_1m
  DROP CONSTRAINT required_buy_usd,
  DROP CONSTRAINT required_sell_usd,
  DROP CONSTRAINT required_size_buckets;

ALTER TABLE mkt.flow_bars_1m
  ADD CONSTRAINT required_buy_usd CHECK (
    buy_usd IS NOT NULL OR coalesce(document#>>'{null_reasons,buy_usd}' IN
      ('NOT_SUPPORTED','SOURCE_FAILED','INCOMPLETE','UNVERIFIED_SEMANTICS',
       'NOT_APPLICABLE','NOT_ESTIMABLE','PENDING'), false)
  ) NOT VALID,
  ADD CONSTRAINT required_sell_usd CHECK (
    sell_usd IS NOT NULL OR coalesce(document#>>'{null_reasons,sell_usd}' IN
      ('NOT_SUPPORTED','SOURCE_FAILED','INCOMPLETE','UNVERIFIED_SEMANTICS',
       'NOT_APPLICABLE','NOT_ESTIMABLE','PENDING'), false)
  ) NOT VALID,
  ADD CONSTRAINT required_size_buckets CHECK (
    (size_buckets IS NOT NULL AND size_buckets <> 'null'::jsonb)
    OR coalesce(document#>>'{null_reasons,size_buckets}' IN
      ('NOT_SUPPORTED','SOURCE_FAILED','INCOMPLETE','UNVERIFIED_SEMANTICS',
       'NOT_APPLICABLE','NOT_ESTIMABLE','PENDING'), false)
  ) NOT VALID;
