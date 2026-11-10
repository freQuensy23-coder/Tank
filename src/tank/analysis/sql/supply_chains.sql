-- supply_chains (PostgreSQL): the stablecoin supply of each chain of the token's chain-liquidity observations and its
-- 24-hour and 7-day change (03 section 5.6 chain liquidity).
-- Rule: per chain the observation is the latest CHAIN_LIQUIDITY or STABLECOIN_CHAIN row of the token at or before the
-- cutoff that was recorded and received at or before the assembly time. The supply at an instant is the `supply_usd` of
-- the latest point of its `series` at or before it and not more than 86400 s before it (the later of two points of one
-- instant); the points are taken at the cutoff, a day and 7 days before it, and the changes are the differences to the
-- supply at the cutoff. A chain whose observation is not available gives the reason and no numbers.
-- One result row per chain, the earliest observation first: the chain, the time, availability and reason of its
-- observation, the supply at the cutoff and the two changes, None where there is none.
-- Shape: one lookup per chain from the (token_id, ts) index and the unnesting of its series.
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
    SELECT DISTINCT ON (s.chain) s.chain, s.ts, s.available, s.reason, s.series
    FROM mkt.supply_obs s, cfg
    WHERE s.token_id = cfg.token_id
      AND s.kind IN ('CHAIN_LIQUIDITY', 'STABLECOIN_CHAIN')
      AND s.ts <= cfg.cutoff
      AND (s.recorded_at IS NULL OR s.recorded_at <= cfg.assembled)
      AND (s.received_ts IS NULL OR s.received_ts <= cfg.assembled)
    ORDER BY s.chain, s.ts DESC, s.storage_key DESC
),
points AS (
    SELECT l.chain, (e.v ->> 'ts')::timestamptz AS at, e.n, (e.v ->> 'supply_usd')::numeric AS usd
    FROM latest l,
         jsonb_array_elements(CASE WHEN jsonb_typeof(l.series) = 'array' THEN l.series ELSE '[]'::jsonb END)
             WITH ORDINALITY AS e(v, n)
    WHERE l.available IS DISTINCT FROM false
),
targets AS (
    SELECT l.chain, t.target
    FROM latest l, cfg,
         LATERAL (VALUES (cfg.cutoff), (cfg.cutoff - interval '1 day'), (cfg.cutoff - interval '7 days')) AS t(target)
),
supply AS (
    SELECT g.chain, g.target, p.usd
    FROM targets g
    CROSS JOIN LATERAL (
        SELECT q.usd
        FROM points q
        WHERE q.chain IS NOT DISTINCT FROM g.chain
          AND q.at <= g.target
          AND q.at >= g.target - interval '86400 seconds'
        ORDER BY q.at DESC, q.n DESC
        LIMIT 1
    ) p
)
SELECT l.chain,
       l.ts AS observed_at,
       l.available,
       l.reason,
       now_.usd AS supply_usd,
       now_.usd - day_.usd AS supply_chg_24h,
       now_.usd - week_.usd AS supply_chg_7d
FROM latest l
CROSS JOIN cfg
LEFT JOIN supply now_ ON now_.chain IS NOT DISTINCT FROM l.chain AND now_.target = cfg.cutoff
LEFT JOIN supply day_ ON day_.chain IS NOT DISTINCT FROM l.chain AND day_.target = cfg.cutoff - interval '1 day'
LEFT JOIN supply week_ ON week_.chain IS NOT DISTINCT FROM l.chain AND week_.target = cfg.cutoff - interval '7 days'
ORDER BY l.ts, l.chain
