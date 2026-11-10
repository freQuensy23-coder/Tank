-- supply_inventory (PostgreSQL): the 24-hour change of the exchange inventory of a token and its 90-day normal range,
-- per window (03 section 0 normal range, section 5.6 exchange inventory).
-- Rule: the inventory observations are the EXCHANGE_INVENTORY and INVENTORY rows of the token at or before the cutoff and
-- within `lookback_s` of it that were recorded and received at or before `assembled`; the current one is the latest, the
-- previous one the latest at or before the cutoff minus a day and not more than 21600 s before that instant. A balance
-- of an observation counts when it is `complete` and has a `qty_native`; its key is (venue, chainid, contract,
-- wallet_list_commit), the last balance of a key winning. The cohort is the keys of the current and of the previous
-- observation when that is available; the change of a key is its current minus its previous quantity, the change of the
-- cohort the same over the sums of the cohort (`base` the previous sum) and `pct` the change over `base`, None where it
-- is 0. A history sample is the same change over a day, (total - total a day earlier) / total a day earlier, of the
-- cohort at an observation of the look-back and the latest observation at or before its day earlier and not more than
-- 21600 s before that, where both have every key of the cohort and the earlier total is not 0; a window counts the
-- samples of the observations before its start and gives their 0.025 and 0.975 quantiles (linear interpolation).
-- One result row per window: the instants of the current and the previous observation and of the first one, the numbers
-- of balances (observed, complete, in the cohort), the cohort sums, the change, the samples of the window, and the cohort
-- by key as parallel lists in the order of the key's text. The caller decides from the first and the current instant
-- whether the history spans the minimum days.
-- Shape: the observations of the token from the (token_id, ts) index, their balances unnested once, one lookup of the
-- earlier observation per observation.
-- param token_id = 'morpho:MORPHO'
-- param cutoff = '2026-11-07 11:58:00+00'
-- param assembled = '2026-11-07 12:13:09.638965+00'
-- param lookback_s = 7776000
-- param names = ARRAY['W15M','W1H','W4H','W24H','EVENT']
-- param starts = ARRAY['2026-11-07 11:43:00+00','2026-11-07 10:58:00+00','2026-11-07 07:58:00+00','2026-11-06 11:58:00+00','2026-11-06 15:33:53.297+00']::timestamptz[]
WITH
cfg AS (
    SELECT %(token_id)s::text AS token_id,
           %(cutoff)s::timestamptz AS cutoff,
           %(assembled)s::timestamptz AS assembled,
           %(lookback_s)s::float8 * interval '1 second' AS lookback
),
inv AS MATERIALIZED (
    SELECT s.storage_key, s.ts, s.available, s.balances
    FROM mkt.supply_obs s, cfg
    WHERE s.token_id = cfg.token_id
      AND s.kind IN ('EXCHANGE_INVENTORY', 'INVENTORY')
      AND s.ts <= cfg.cutoff
      AND s.ts >= cfg.cutoff - cfg.lookback
      AND (s.recorded_at IS NULL OR s.recorded_at <= cfg.assembled)
      AND (s.received_ts IS NULL OR s.received_ts <= cfg.assembled)
),
bal AS MATERIALIZED (
    SELECT DISTINCT ON (i.storage_key, k.key)
           i.storage_key, k.key, k.venue, k.chainid, k.contract, k.commit, k.qty
    FROM inv i
    CROSS JOIN LATERAL (
        SELECT jsonb_build_array(e.v -> 'venue', e.v -> 'chainid', e.v -> 'contract', e.v -> 'wallet_list_commit') AS key,
               e.v ->> 'venue' AS venue,
               (e.v ->> 'chainid')::bigint AS chainid,
               e.v ->> 'contract' AS contract,
               e.v ->> 'wallet_list_commit' AS commit,
               (e.v ->> 'qty_native')::numeric AS qty,
               e.n
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(i.balances) = 'array' THEN i.balances ELSE '[]'::jsonb END)
             WITH ORDINALITY AS e(v, n)
        WHERE e.v -> 'complete' = 'true'::jsonb
          AND e.v ->> 'qty_native' IS NOT NULL
    ) k
    ORDER BY i.storage_key, k.key, k.n DESC
),
cur AS (
    SELECT storage_key, ts FROM inv ORDER BY ts DESC, storage_key DESC LIMIT 1
),
prev AS (
    SELECT p.storage_key, p.ts, p.available
    FROM inv p, cfg
    WHERE p.ts <= cfg.cutoff - interval '1 day'
      AND p.ts >= cfg.cutoff - interval '1 day' - interval '21600 seconds'
    ORDER BY p.ts DESC, p.storage_key DESC
    LIMIT 1
),
cohort AS MATERIALIZED (
    SELECT n.key, n.venue, n.chainid, n.contract, n.commit, n.qty AS now_qty, o.qty AS old_qty,
           '(' || coalesce('''' || n.venue || '''', 'None') || ', ' || coalesce(n.chainid::text, 'None') || ', '
               || coalesce('''' || n.contract || '''', 'None') || ', ' || coalesce('''' || n.commit || '''', 'None') || ')' AS sort_key
    FROM bal n
    JOIN bal o ON o.key = n.key
    WHERE n.storage_key = (SELECT storage_key FROM cur)
      AND o.storage_key = (SELECT storage_key FROM prev)
      AND (SELECT available IS DISTINCT FROM false FROM prev)
),
tot AS MATERIALIZED (
    SELECT b.storage_key, sum(b.qty) AS qty
    FROM bal b
    JOIN cohort c ON c.key = b.key
    GROUP BY b.storage_key
    HAVING count(*) = (SELECT count(*) FROM cohort)
),
hist AS (
    SELECT t.ts, ((t.qty - o.qty) / o.qty)::float8 AS x
    FROM (SELECT r.ts, r.storage_key, tr.qty FROM inv r JOIN tot tr ON tr.storage_key = r.storage_key) t
    CROSS JOIN LATERAL (
        SELECT p.storage_key
        FROM mkt.supply_obs p, cfg
        WHERE p.token_id = cfg.token_id
          AND p.kind IN ('EXCHANGE_INVENTORY', 'INVENTORY')
          AND p.ts <= t.ts - interval '1 day'
          AND p.ts >= t.ts - interval '1 day' - interval '21600 seconds'
          AND p.ts >= cfg.cutoff - cfg.lookback
          AND (p.recorded_at IS NULL OR p.recorded_at <= cfg.assembled)
          AND (p.received_ts IS NULL OR p.received_ts <= cfg.assembled)
        ORDER BY p.ts DESC, p.storage_key DESC
        LIMIT 1
    ) pp
    JOIN tot o ON o.storage_key = pp.storage_key
    WHERE o.qty <> 0
),
venues AS (
    SELECT array_agg(venue ORDER BY sort_key COLLATE "C") AS venues,
           array_agg(chainid ORDER BY sort_key COLLATE "C") AS chainids,
           array_agg(contract ORDER BY sort_key COLLATE "C") AS contracts,
           array_agg(now_qty ORDER BY sort_key COLLATE "C") AS qty_native,
           array_agg(now_qty - old_qty ORDER BY sort_key COLLATE "C") AS chg_native,
           array_agg(CASE WHEN old_qty <> 0 THEN ((now_qty - old_qty) / old_qty)::float8 END ORDER BY sort_key COLLATE "C") AS chg_pct
    FROM cohort
)
SELECT w.name AS "window",
       (SELECT ts FROM cur) AS current_at,
       (SELECT ts FROM prev) AS previous_at,
       (SELECT min(ts) FROM inv) AS first_at,
       (SELECT jsonb_array_length(balances) FROM inv WHERE storage_key = (SELECT storage_key FROM cur)
            AND jsonb_typeof(balances) = 'array') AS observed_balances,
       (SELECT count(*) FROM bal WHERE storage_key = (SELECT storage_key FROM cur)) AS complete_balances,
       (SELECT count(*) FROM cohort) AS comparable_balances,
       (SELECT sum(old_qty) FROM cohort) AS base,
       (SELECT sum(now_qty) FROM cohort) AS total,
       (SELECT sum(now_qty - old_qty) FROM cohort) AS change,
       (SELECT CASE WHEN sum(old_qty) <> 0 THEN (sum(now_qty - old_qty) / sum(old_qty))::float8 END FROM cohort) AS pct,
       count(h.x) AS samples,
       percentile_cont(0.025) WITHIN GROUP (ORDER BY h.x) AS normal_low,
       percentile_cont(0.975) WITHIN GROUP (ORDER BY h.x) AS normal_high,
       coalesce((SELECT venues FROM venues), '{}') AS venues,
       coalesce((SELECT chainids FROM venues), '{}') AS chainids,
       coalesce((SELECT contracts FROM venues), '{}') AS contracts,
       coalesce((SELECT qty_native FROM venues), '{}') AS qty_native,
       coalesce((SELECT chg_native FROM venues), '{}') AS chg_native,
       coalesce((SELECT chg_pct FROM venues), '{}') AS chg_pct
FROM unnest(%(names)s::text[], %(starts)s::timestamptz[]) WITH ORDINALITY AS w(name, start, n)
LEFT JOIN hist h ON h.ts < w.start
GROUP BY w.n, w.name
ORDER BY w.n
