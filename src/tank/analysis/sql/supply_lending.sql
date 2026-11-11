-- supply_lending (PostgreSQL): the 24-hour change of the utilization of each lending pool of the token's latest
-- lending observation (03 section 5.6 lending).
-- Rule: the latest observation is the latest LENDING row of the token at or before the cutoff that was recorded and
-- received at or before the assembly time; the earlier one is the latest available LENDING row at or before the cutoff
-- minus a day and not more than 1800 s before that instant, matched by `pool_id` (the last pool of an id winning). A
-- pool's change is its utilization minus the earlier one, in float, None where either is missing.
-- One result row per pool id of the latest observation: the id and the change.
-- Shape: two lookups from the (token_id, ts) index and the unnesting of their pools.
-- param token_id = 'morpho:MORPHO'
-- param cutoff = '2026-11-07 11:58:00+00'
-- param assembled = '2026-11-07 12:13:09.638965+00'
WITH
cfg AS (
    SELECT %(token_id)s::text AS token_id,
           %(cutoff)s::timestamptz AS cutoff,
           %(assembled)s::timestamptz AS assembled
),
latest AS (
    SELECT s.pools
    FROM mkt.supply_obs s, cfg
    WHERE s.token_id = cfg.token_id
      AND s.kind = 'LENDING'
      AND s.ts <= cfg.cutoff
      AND (s.recorded_at IS NULL OR s.recorded_at <= cfg.assembled)
      AND (s.received_ts IS NULL OR s.received_ts <= cfg.assembled)
    ORDER BY s.ts DESC, s.storage_key DESC
    LIMIT 1
),
earlier AS (
    SELECT s.pools
    FROM mkt.supply_obs s, cfg
    WHERE s.token_id = cfg.token_id
      AND s.kind = 'LENDING'
      AND s.available IS DISTINCT FROM false
      AND s.ts <= cfg.cutoff - interval '1 day'
      AND s.ts >= cfg.cutoff - interval '1 day' - interval '1800 seconds'
      AND (s.recorded_at IS NULL OR s.recorded_at <= cfg.assembled)
      AND (s.received_ts IS NULL OR s.received_ts <= cfg.assembled)
    ORDER BY s.ts DESC, s.storage_key DESC
    LIMIT 1
),
before AS (
    SELECT DISTINCT ON (e.v ->> 'pool_id') e.v ->> 'pool_id' AS pool_id, (e.v ->> 'utilization')::float8 AS utilization
    FROM earlier,
         jsonb_array_elements(CASE WHEN jsonb_typeof(earlier.pools) = 'array' THEN earlier.pools ELSE '[]'::jsonb END)
             WITH ORDINALITY AS e(v, n)
    ORDER BY e.v ->> 'pool_id', e.n DESC
)
SELECT DISTINCT ON (p.v ->> 'pool_id')
       p.v ->> 'pool_id' AS pool_id,
       (p.v ->> 'utilization')::float8 - b.utilization AS chg_24h
FROM latest,
     jsonb_array_elements(CASE WHEN jsonb_typeof(latest.pools) = 'array' THEN latest.pools ELSE '[]'::jsonb END)
         WITH ORDINALITY AS p(v, n)
LEFT JOIN before b ON b.pool_id = p.v ->> 'pool_id'
WHERE p.v ->> 'pool_id' IS NOT NULL
ORDER BY p.v ->> 'pool_id', p.n DESC
