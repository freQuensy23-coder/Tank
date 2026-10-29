# Tank data guide — current subscription/Compose/DuckDuckGo baseline



| Topic | Rule |
|---|---|
| IDs | ULID-based, typed prefixes: `ITM-` item (`web-<hash>` for a page read with `fetch_page` or a `web_search` result), `EVT-` event, `OBS-` observation, `CLM-` claim, `EMS-`, `EDG-` beneficiary edge, `MTA-` Market Truth assessment, `DEC-` decision, `CND-` watch condition, `CAL-` call, `ALR-` alert (+ integer version), `RWP-` reaction pack, `JOB-`, `PRP-` proposal, `FIX-` fixture. `source_id`, `entity_id`, `token_id`, `instrument_id` are readable registry keys (e.g. `binance:spot:UNIUSDT`). Evidence bundles are content-addressed: `EVB-<sha256>`. Tool arguments take these IDs and keys; how to find them: «Finding record IDs for tool arguments» below. |
| Time | UTC everywhere; stored with microsecond resolution. Every timestamp field has a sibling precision: `EXACT`, `BUCKET(<size>)`, `COLLECTION_ONLY`. |
| Units | Native token quantity and USD notional are separate decimal fields. Returns are decimal fractions. Funding is stored as rate per interval + `interval_h`, with derived `rate_8h_eq`. Basis in bps. |
| Missing data | Never 0. Every measurable is `Measured{value, unit, window, observed_at, precision, coverage, reason}`; when `value = null`, `reason ∈ {NOT_SUPPORTED, SOURCE_FAILED, INCOMPLETE, NOT_APPLICABLE, NOT_ESTIMABLE, PENDING}`. |
| Scores | 0–10 rubric levels (§4.1–4.2, §4.5, §4.7), or `null` with a reason. Scores are never averaged into one number. |
| Normal range | For a per-token measure with base-tier history — BTC-adjusted return, volume, native and USD open-interest change, funding, exchange-inventory change, BTC realized volatility — the normal range is the middle 95% of the token's own values for the same window length over its stored history (up to 90 days, 01 §9; at least 7 days, one weekly cycle, else n/a). "Beyond normal" means outside it. Each token is judged against itself; 95% is the usual significance level. |
| Majority | "Most" means more than half — of volume, of net flow, of a USD change. |
| Significant flow | A net flow counts as BUY or SELL only when `\|CVD\| ≥ 2·√Σq²` over its trades (q = native size of each trade): with random aggressor sides this happens in only 5% of windows. Otherwise it is NEUTRAL. |
| Versioning | Every derived record stores `config_version` and `code_version`; every agent-derived record also stores `model_id`, `prompt_version` (the agent and skill version in OpenMA) and the OpenMA session id, whose log holds the raw responses. |

---




Notation: `field: type` · `?` = nullable · `[T]` = list · comments after `#`.

### 2.1 Registries

```
SourceRegistryEntry
  source_id: str                     # slug, e.g. "sec_press"
  name, provider, endpoint: str
  bucket: SourceBucket
  priority: SourcePriority
  capabilities: [str]                # announcements, filings, governance, trades, oi, funding, ...
  mode: RSS | ATOM | REST | WS | GRAPHQL | HTML_DIFF | SOCIAL_API | BROWSER | FILE | RPC
  latency_class: REALTIME | NEAR_REALTIME | DELAYED | HISTORICAL
  historical_depth: str
  cost_class: FREE_OPEN | FREE_ACCOUNT | FREE_TIER | PAID
  paid_gap_ref: str?                 # required when cost_class = PAID
  auth: NONE | API_KEY | OAUTH | BROWSER_SESSION
  limits: {rate_per_min: int?, burst: int?, pagination: str?, retention: str?}
  source_class: SourceClass
  redundancy_tier: 0 | 1 | 2
  headless_ok: bool
  terms_notes: str
  expected_cadence_s: int
  adapter_status: AdapterStatus

Entity
  entity_id: str
  kind: COMPANY | PROTOCOL | REGULATOR | ISSUER | EXCHANGE | CUSTODIAN | CHAIN | DAO | OTHER
  names: [str]; aliases: [str]
  tokens: [token_id]
  official_sources: [source_id]      # where the Verifier looks first
  github_repos: [owner/repo]         # S25 repositories this entity publishes; routing identity (§3.1)
  layers_served: [StackLayer]        # dependency map
  competes_with: [entity_id]
  chains: [str]

Token
  token_id: str                      # e.g. "uniswap:UNI"
  symbol, name: str
  entity_id: str?
  sector: str?
  chains: [str]

Instrument
  instrument_id: str                 # e.g. "okx:perp_linear:UNI-USDT-SWAP"
  token_id, venue: str
  market_type: MarketType
  venue_symbol, quote_asset: str
  contract_multiplier: decimal?      # base units per contract (perps)
  funding_interval_h: decimal?
  tick_size, lot_size: decimal
  active: bool; resolved_at: ts

SectorBasket
  sector: str; constituents: [token_id]; weighting: EQUAL
```

### 2.2 Ingestion

```
SourceItem
  item_id
  source_id
  external_id: str?                  # GUID, post ID, filing accession, commit SHA
  raw_ref: str; raw_sha256: str
  content_hash: str                  # hash of normalized text, for exact-duplicate detection
  url: str?
  title, text: str                   # normalized
  published_at: ts? (+precision)
  observed_at: ts
  source_class: SourceClass
  relevance: {decision: ALWAYS | SEND | SKIP, p_relevant: 0..1?, model: str?}   # §3.1
  route: {decision: NEW_SESSION | ADDED, event_id: str?, p_same: 0..1?}?        # §3.1
  extraction_status: PENDING | DONE | SKIPPED | FAILED
  extraction_reason: str?
  # SKIPPED: rejected by the agent (record_event is_event=false, its no_event_reason) or by the filter.
  # A turn that ends without record_event, add_observation or merge_into for the item leaves it PENDING
  # with extraction_reason AGENT_REMINDED while Tank reminds the agent; FAILED with AGENT_INCOMPLETE
  # once the session's reminders are spent (tank reanalyse can queue it again).
```

### 2.3 Events

```
Event
  event_id
  statement: str                     # one neutral factual sentence
  theme_tags: [str]                  # obvious themes only; never scored
  entities: [{entity_id | "UNRESOLVED:<name>", role: ACTOR | COUNTERPARTY | SUBJECT | REGULATOR}]
  tokens_mentioned: [token_id]
  event_time: ts? (+precision)       # T0
  public_at: ts (+precision, basis)  # T1: when the event really became public (model estimate, §3.4)
  known_to_us_at: ts                 # permanent; only ever moves earlier (merge)
  verified_at: ts?
  freshness: Freshness
  event_state: EventState            # highest stage stated by a HIGH or MEDIUM claim
  state_verified: bool               # true when that stage rests on a HIGH-certainty claim
  negative_marker: NegativeMarker?
  certainty: Certainty               # facts
  timing_confidence: Certainty
  best_source_class: SourceClass
  next_milestone: {kind: str, due_at: ts, source_id: str}?
  lifecycle: EventLifecycle
  merged_into: event_id?
  current_ems_id: str?

EventObservation
  obs_id, event_id, item_id
  link_type: ORIGIN | CORROBORATION | STATE_UPDATE | CONTRADICTION | DENIAL
  public_at: ts; freshness: Freshness   # model estimate for this item (§3.4)
  match_method: EXTERNAL_ID | EXACT_HASH | AGENT
  observed_at: ts

Claim
  claim_id, obs_id
  type: FACT | STATE | NUMBER | RELATION | TIMESTAMP | MECHANISM | MILESTONE
  subject: entity_id | str; predicate: str; object: str
  value: {number: decimal, unit: str}?
  state_claim: EventState | IntegrationState | NegativeMarker | null
  directionality: Directionality
  evidence: [{item_id, quote: str}]  # quote must be a verbatim substring of the item text
  evidence_class: EvidenceClass
  verified: bool                     # rests on a HIGH-certainty source (§4.4)
  accepted: bool; rejection_reason: str?

StateChange                          # append-only
  event_id, edge_id?
  ladder: EVENT | INTEGRATION
  from_state, to_state
  evidence: [claim_id]
  changed_at: ts

ImpactHorizon
  impact_start: ImpactStart
  ramp_shape: RampShape
  dependencies: [str]
  time_to_materiality: str
  timing_confidence: Certainty

EMSAssessment
  ems_id, event_id, version, cutoff
  dims: {
    economic_directness, magnitude, permanence, distribution_expansion,
    mechanical_flows, surprise, implementation : DimScore }
  structural_gates: [{gate: G1..G6, satisfied: bool, evidence: [claim_id]}]
  class: EMSClass
  criterion: G1..G6 | M1..M7 | MATERIAL | LOW; criterion_claim_id: claim_id?   # 01 §6.3
  criterion_quote: str               # the quote of criterion_claim_id, stored with it
  criterion_venue_id: entity_id?     # M1 only
  class_reasons: [str]               # e.g. "M2", "cap: LOW certainty"
  impact_horizon: ImpactHorizon

DimScore
  value: 0..10?; reason: str?
  rationale: str
  evidence: [claim_id]
  proposed_by: LLM | RULE
  caps_applied: [rule_id]

CaptureChain                         # per event × token
  event_id, token_id, version
  headline_activity: Measured        # what is advertised/reported
  eligible_activity: Measured        # portion actually subject to fees/mechanism
  effective_fees: Measured           # after discounts, rebates, incentives, exemptions
  protocol_capture: Measured; capture_recipient: str?
  token_mechanism: {
    type: BUYBACK | BURN | STAKING_REWARD | FEE_SHARE | GAS | COLLATERAL | REQUIRED_LOCK | NONE | UNKNOWN,
    active: bool, conditional_on: str?, evidence: [claim_id] }
  token_flow_annual_usd: Measured
  token_flow_pct_mcap: Measured
  capture_score: 0..10?
  capture_class: CaptureClass
  conditional: bool
  caps_applied: [rule_id]
```

### 2.4 Beneficiaries

```
BeneficiaryEdge
  edge_id, event_id, map_version
  entity_id, token_id?
  role: BeneficiaryRole
  layer: StackLayer
  direction: POSITIVE | NEGATIVE
  causal_chain: str
  integration_state: IntegrationState?   # highest verified; null for PRIMARY (event state applies)
  directionality: Directionality
  exposure_score: 0..10
  capture_ref: (event_id, token_id, version)
  tradable: bool                     # instruments on covered venues
  edge_certainty: Certainty
  credible: bool; credibility_reasons: [str]
  evidence: [claim_id]

BeneficiaryMap
  event_id, map_version, cutoff
  layers: {StackLayer: {status: FOUND | NONE | UNKNOWN, edges: [edge_id], note: str}}
  least_repriced_edge: edge_id?; least_repriced_reason: str?
  no_credible_secondary_reason: str?
  analysis_set: [token_id]           # tokens sent to Market Truth
```

### 2.5 Market data

```
MetricSemantics
  metric_key: str                    # "<venue>.<market_type>.<field>"
  venue, market_type, endpoint_or_stream: str
  meaning: str
  unit: BASE_QTY | CONTRACTS | QUOTE_NOTIONAL | USD_NOTIONAL | RATE_PER_INTERVAL | PRICE | BPS | COUNT
  contract_multiplier_ref: instrument_id?
  sidedness: SINGLE_SIDED | BOTH_SIDES | NOT_PUBLISHED | N_A
  side_semantics: TAKER_SIDE | MAKER_SIDE | BUYER_IS_MAKER_FLAG | N_A
  trade_aggregation: PER_FILL | PER_TAKER_ORDER_SAME_PRICE | N_A
  scope: str                         # per instrument / per venue / provider aggregate
  calc_method: str                   # snapshot, bucket sum, settled value, estimate, ...
  timestamp_semantics: EXCHANGE_EVENT_TIME | BUCKET_START | BUCKET_END | SETTLEMENT_TIME | COLLECTION_TIME
  timestamp_unit: S | MS | US
  timestamp_precision: EXACT | BUCKET | COLLECTION_ONLY
  price_contaminated: bool
  requires_native_normalization: bool
  aggregation_class: str             # only identical classes may be combined
  inference_limits: [str]            # what this metric cannot prove
  verified_at: ts?; verified_by: str?; doc_ref: str?
  critical: bool

FlowBar1m                            # per instrument per minute (hot tier, from trades)
  instrument_id, minute_start
  buy_qty, sell_qty                  # native, aggressor side
  buy_notional, sell_notional        # quote → USD
  n_buy, n_sell
  sum_sq_qty                         # Σ q² of the minute's trades (§0 significant flow)
  open, high, low, close, vwap       # trade-based
  size_buckets: {B1..B5: {buy_qty, sell_qty, buy_notional, sell_notional, n}}
  first_trade_id, last_trade_id
  complete: bool; gap_reason: str?

Kline1m                              # base tier
  instrument_id, open_time, o, h, l, c
  volume_native, quote_volume, n_trades
  taker_buy_native?, taker_buy_quote?
  complete: bool

BookSummary                          # hot tier, every ⚙10 s per venue
  instrument_id, exchange_ts, received_ts
  mid, best_bid, best_ask, spread_bps
  depth: {bid|ask: {p0_5|p1|p2: {native, usd}?}}   # null when the band is not covered
  range_covered_pct: {bid, ask}
  levels_received: int
  integrity_ok: bool                 # sequence/checksum where the venue provides it

OIObservation
  instrument_id, ts (+precision), received_ts
  raw_value, raw_unit, sidedness, metric_key
  oi_native: decimal?                # normalized via semantics (contracts × multiplier)
  oi_usd: decimal?                   # reported, or oi_native × mark

FundingObservation
  instrument_id
  kind: SETTLED | CURRENT_ESTIMATE
  rate, interval_h, rate_8h_eq
  settlement_time | as_of: ts
  metric_key

LiquidationObservation
  instrument_id, ts, side: LONG_LIQ | SHORT_LIQ
  qty_native, notional_usd, price
  coverage: LiqCoverage
```

### 2.6 Analysis outputs

```
SpotTruth                            # per token × window × cutoff
  window: W15M | W1H | W4H | W24H | EVENT
  venues: [{venue, complete: bool, excluded_reason: str?, volume_native, buy_qty, sell_qty,
            cvd, buy_share, bs_ratio, volume_share, sign: -1|0|+1}]
  composite_max: {cvd, buy_share, bs_ratio, volume, venues: [str]}
  composite_cohort: {cohort: [str], cvd, buy_share, volume, velocity, share_of_max}?
  breadth: {n_pos, n_neg, n_neutral, pct_pos, pct_neg}
  cw_breadth: {buy, sell, neutral}
  topn: {n, venues: [str], agreement}
  concentration: {hhi, top_volume_venue, top_volume_share, top_contrib_venue, top_contrib_share, others_cwb_dir}
  direction: SpotDirection
  classification: SpotClass
  coverage: {covered_share_est, n_complete, excluded: [{venue, reason}]}
  size_profile: {large_ticket_buy_share, large_ticket_sell_share, compatible_venues: [str]}?
  cohort_consistent: {<other window>: bool}

DerivativesTruth
  per_venue: [{venue, oi_native_start, oi_native_end, oi_native_chg_pct, oi_usd_chg_pct,
               decomposition: {position, markup, interaction, pos_share}, oi_class,
               funding_settled_last, funding_estimate, interval_h, funding_8h_eq, funding_vs_normal,
               basis_bps, perp_volume_usd, perp_taker_buy_share,
               liq_long_usd, liq_short_usd, liq_coverage}]
  synchronized: {venues: [str], oi_native_chg_pct, oi_usd_chg_pct, oi_class, decomposition}?
  funding_dispersion_8h: decimal?
  liquidations: {complete_venues_total: {long, short}, sampled_venues_min: {long, short}}
  incremental_share: {perp, spot}?
  lead_lag: {first_mover: SPOT | PERP | SIMULTANEOUS | UNKNOWN, spot_confirmed: bool?, sequence: [{venue, market, t_first}]}

LiquidityTruth
  per_venue: [{venue, spread_bps, depth_p1_usd, depth_p2_usd?, range_covered_pct,
               slippage_bps: {T1, T2, T3}, tier_notional: {T1, T2, T3}}]
  aggregate_depth_p1_usd, pre_event_depth_p1_usd?
  depth_drop: decimal?; spread_ratio: decimal?
  replenishment: {available: bool, bursts, replenished, median_recovery_s, resilient_bursts}
  elasticity: {bps_per_depth_unit, weak_response: bool, excess_response: bool}
  vol_compression: {rv24, normal_range, state: COMPRESSED | NORMAL | EXPANDED}
  liquidity_state: LiquidityState

SupplyContext
  exchange_inventory: {chg_native_24h, chg_pct, coverage}?   # null + reason when unavailable
  flows: [{class: FRESH_ACCUMULATION | TREASURY_INTERNAL | MM_INVENTORY | CEX_CUSTODY_MIGRATION |
           UNLOCK_DISTRIBUTION | OTC_SETTLEMENT | AMBIGUOUS, qty, ts, evidence}]
  unlocks_next_14d: [{date, qty, pct_circulating, recipient_class, transferable: bool?}]
  lending: {utilization, borrow_apr, chg_24h}?
  stablecoin_chain: {chain, supply_chg_24h, supply_chg_7d}?
  supply_in_evidence: TRUE | FALSE | UNKNOWN

RelativePrice
  ref_venue: str
  anchor: {type: T0 | T1 | KNOWN_TO_US_AT, ts, ref_price}
  windows: {W15M|W1H|W4H|W24H|EVENT: {r_raw, r_btc_adj, r_eth_adj, r_sector_adj, pctile}}
  beta_btc, beta_eth: decimal?
  pre_event_runup: {ar, pctile, flag: bool}
  repricing_band: RepricingBand
  market_context: MarketContext; context_inputs: {...}

DataConfidence
  class: LOW | MEDIUM | HIGH          # §5.10 conditions
  failed: [{condition, reason}]      # conditions that keep it from HIGH
  limiting: {condition, reason}      # printed as the card's "main unknown"

MarketTruthAssessment
  mta_id, token_id, event_id?, cutoff, primary_window
  spot: {window: SpotTruth}
  derivatives: DerivativesTruth | NOT_APPLICABLE
  liquidity: LiquidityTruth
  supply: SupplyContext
  relative: RelativePrice
  move_class: MoveClass; support: int; alternatives: [MoveClass]
  divergence: {state: DivergenceState,
               candidates: [{state, eligible, refuted, testable, support}],
               unresolved: [DivergenceState]}
  flags: [PRICE_MARKUP_OI | NOT_COHORT_CONSISTENT | PRE_EVENT_RUNUP | FRAGMENTATION_INVESTIGATE | ...]
  decisive_facts: [str]              # 2–3 facts selected for the card
  data_confidence: DataConfidence
  class_confidence: Certainty

MovementOutlook                      # stored as the decision's `outlook`, no id of its own
  direction: DirectionBias; direction_rule: str      # §6.1 row matched
  potential: MovePotential; potential_rule: str      # §6.2 row matched
  best_horizon: Horizon; horizon_reason: str
  scenarios: {base, upside, failure}                 # sentences, no numbers
  confidence: Certainty; caps: {data, event, market_truth}
  tactical: bool
  conditions: {upgrade: [cond_id], downgrade: [cond_id]}
```

### 2.7 Decision, alert and audit

```
Contradiction
  contradiction_id, event_id, token_id?
  type: ContradictionType
  description: str; evidence: [claim_id | metric ref]
  detected_at; resolved_at?; resolution: str?

WatchCondition
  cond_id, event_id, token_id
  role: UPGRADE | DOWNGRADE | INVALIDATION | MILESTONE
  expr: {metric: str, window: str, op: str, value: any, persist: {count: int, unit: str}}
     | {milestone: str, due_at: ts}
  human_text: str
  created_by: dec_id
  expires_at: ts
  status: ARMED | FIRED | EXPIRED | CANCELLED; fired_at: ts?

Decision
  dec_id, event_id, token_id, cutoff
  thesis_grade: Grade; thesis_rule: str             # §4.8 row matched
  trade_grade: Grade; trade_reasons: [rule_id]
  action: Action; action_rule: rule_id; tactical: bool; investigate: bool
  call_id: str?
  contradictions: [contradiction_id]
  conditions_shown: [cond_id]        # at most 2

Call                                 # open-call store
  call_id, event_id, token_id
  opened_by: (alert_id, version); opened_at
  entry_ref_price: decimal           # reference-venue price at card generation
  invalidation_price: decimal
  status: OPEN | CLOSED; closed_by: EXIT | EXPIRED; closed_at?

Alert                                # append-only; one row per version
  alert_id, version, kind: AlertKind
  event_id, generated_at
  card_text: str; card_struct: {section: content}
  decisions: [dec_id]
  evidence_bundle_id: str
  supersedes_version: int?

DeliveryRecord                       # append-only ledger; one row per transition
  subject: (alert_id, version) | notice_id    # exactly one of the two
  channel, status: DeliveryStatus
  message_ref: str?; attempt: int; at: ts; error: str?

OpsNotice                            # append-only; one "Tank Ops" message
  notice_id, kind: OpsNoticeKind
  subject_id: str?                   # alert_id of a DELIVERY_FAILED
  text: str; created_at
  created_by: scheduler (HEARTBEAT) | delivery (DELIVERY_FAILED)

EvidenceBundle                       # content-addressed JSON
  bundle_id = sha256(canonical_json)
  cutoff, request_log, source_items, observations, claims, state_changes,
  ems, capture_chains, beneficiary_map, market_truth[], outlooks[], decisions[],
  contradictions[], data_confidence[], unresolved_fields[],
  config_hash, code_version, model_ids, agent_version, skill_version, session_id, tool_call_ids

ReactionWindowPack                         # scheduler owns the five POST jobs; PRE windows computed immediately
  rwp_id, event_id, token_id
  anchor: {type: T0 | T1 | KNOWN_TO_US_AT, ts}
  checkpoints: [{label: PRE_24H | PRE_4H | PRE_1H | POST_15M | POST_1H | POST_4H | POST_24H | POST_72H,
                 captured_at, retro: bool, revision: int,
                 r_raw, r_btc_adj, pctile,
                 spot_cvd, spot_method: TRADES | NONE, spot_coverage,
                 oi_native_chg_pct, oi_class,
                 funding: [{venue, settled, estimate}],
                 liquidations: {long, short, coverage}}]
```

### 2.8 Control, sidecar, QA

```
Job
  job_id, type, priority: 0..4 (0 highest)
  idem_key: str (unique)
  payload: json
  status: QUEUED | RUNNING | DONE | FAILED | EXPIRED
  attempts, max_attempts
  run_after, expires_at: ts
  last_error: str?

HotToken                             # hot-set membership (§9)
  token_id, event_id
  priority: int                      # EMS class rank
  since: ts; expires_at: ts?         # null while an open call or armed condition exists

ExpansionProposal
  prp_id, hook: ScoutHook
  concept_name, definition, why_existing_fails, mechanism: str
  evidence: [ref]; overlap: str
  recommendation: ScoutRecommendation; confidence: Certainty
  occurrences: int
  adversarial_review: {overlap, observability, reusability, decision_value, verdict}
  human_action: HumanScoutAction?; decided_at?

ReviewRecord
  review_id, subject: alert_id | event_id
  label: CATCH | LATE | FALSE_ALARM | NOISE | CORRECT_SILENCE | MISS
  miss_reason: LATE | WRONG_BENEFICIARY | BAD_DATA | OLD_NEWS | STATE_ERROR |
               MARKET_TRUTH_ERROR | DELIVERY_FAILURE | SOURCE_NOT_COVERED ?
  notes; reviewer; at

Fixture
  fixture_id, name, cutoff
  anonymized: bool; name_map_ref: str?
  source_items: [ref]; market_data: [ref]; registry_snapshot: ref; config_version
  assertions: [{id, target, op, expected}]
  sealed_outcome_ref: str            # not readable by the pipeline
```

### 2.9 Storage mapping (ClickHouse: exchange series and prints; PostgreSQL: everything else)

| Table | Key | Notes |
|---|---|---|
| `sources`, `entities`, `entity_aliases`, `tokens`, `instruments`, `sector_baskets`, `exchange_addresses` | natural slugs | Loaded from the config repository; version stamped. The dependency map and metric semantics stay in their YAML files; `get_registry` gives agents the dependency map. |
| `source_items` | `item_id`; unique (`source_id`, `external_id`) | Index `content_hash`, `observed_at`. |
| `events` | `event_id` | Index `known_to_us_at`, `lifecycle`. |
| `event_observations`, `claims` | `obs_id`, `claim_id` | FK to events/items. |
| `state_changes` | serial | Append-only. |
| `ems_assessments`, `capture_chains`, `beneficiary_maps`, `beneficiary_edges` | id + version | Versioned rows. |
| `klines_1m`, `flow_bars_1m` | (`instrument_id`, minute) | ClickHouse `mkt.*` (ReplacingMergeTree, daily partitions); base tier 90 days ⚙. |
| `book_summaries` | (`instrument_id`, ts) | ClickHouse; kept 72 hours; summary fields only (no order-book levels). |
| `oi_obs`, `funding_obs`, `liq_obs`, `tickers_24h`, `trades` | (`instrument_id`, ts) | ClickHouse (daily partitions); `trades` (every print) 30 days. |
| `market_truth`, `decisions`, `contradictions`, `watch_conditions`, `calls` | ids | `market_truth` indexed by (`token_id`, `cutoff`); a decision carries its outlook. |
| `alerts`, `deliveries`, `ops_notices`, `evidence_bundles`, `reaction_checkpoints`, `reviews` | ids | Append-only (UPDATE/DELETE rejected by trigger). A reaction checkpoint embeds its whole Market Truth (about 1 MB); its column `metadata` is the same row without `market_truth`: read and filter checkpoint fields through `metadata`, and `document` only when you need the Market Truth. |
| `ops_cards` | (`chat_id`, `item_id`) | The "Tank Ops" card of each material and its Telegram message; mutable, edited as notices arrive (§7.6). |
| `hot_tokens` | (`token_id`, `event_id`) | Hot set (§9); read on start to subscribe the hot tokens again. |
| `jobs` | `job_id`; unique `idem_key` | The service's job queue (§3.8). |
| `agent_sessions`, `tool_calls` | `session_id`, `call_id` | The OpenMA sessions of each event (one live session; closed earlier generations keep their rows and `event_id`, indexed, not unique), and every tool call with its input, result and timing (§3.2). |
| `expansion_proposals` | `prp_id` | Only table the scout role may write, together with its own logs. |
| `fixtures`, `replay_runs` | — | Files, not tables: fixtures are listed in `tests/fixtures/manifest.json`; a replay run freezes its inputs and outputs in its output directory. Sealed outcomes stay outside anything the pipeline reads. |

Core DDL: §14.

### 2.10 Metric Semantics registry — initial content (`semantics.yaml`)

Trade aggressor side, units and IDs:

| `metric_key` | Field | Meaning in Tank | Unit / time | Dedup key | Evidence |
|---|---|---|---|---|---|
| `binance.spot.aggTrade` / `binance.usdm.aggTrade` | `m` | `m = true` → buyer was maker → aggressor SELL | `q` base units; `T` ms | `a` (agg ID; `f`–`l` trade range) | Live sample `"m":false` on WLDUSDT |
| `coinbase.spot.matches` / REST `trades` | `side` | Maker order side → aggressor is the opposite side | `size` base; `time` ISO-8601 µs | `trade_id` | Exchange documentation; live sample `"side":"sell"` |
| `okx.spot.trades` / `okx.swap.trades` | `side` | Taker side | `sz` base (spot) or contracts (swap, × `ctVal`); `ts` ms | `tradeId` | — |
| `bybit.spot.publicTrade` / `bybit.linear.publicTrade` | `S` (WS), `side` (REST) | Taker side | `v`/`size` base; `T`/`time` ms | `i` / `execId` | — |
| `kraken.spot.trade` | `side` (WS v2), `b`/`s` (REST) | Taker side | `qty` base; REST time = seconds (float) | `trade_id` | — |
| `hyperliquid.perp.trades` | `side` | `B` → aggressor BUY, `A` → aggressor SELL | `sz` coin; `time` ms | `tid` | 16 Oct 2026, BTC/ETH/HYPE/SOL against the book: of trades printed at the bid or ask, 104 of 115 `B` at the ask, 49 of 62 `A` at the bid |

Open interest:

| `metric_key` | Field used | Unit / sidedness | Not to be used | Evidence |
|---|---|---|---|---|
| `binance.usdm.openInterest` | `openInterest` | Base units, single-sided (WLDUSDT 16 Oct 2026: 203,999,216 WLD) | — | — |
| `binance.usdm.openInterestHist` | `sumOpenInterest`; `sumOpenInterestValue` | Base units, single-sided; USD notional (price-contaminated); 5m buckets, 30-day history | `CMCCirculatingSupply` (not OI) | — |
| `okx.swap.open-interest` | `oiCcy` | Coin units, single-sided (`oi` = contracts; `oi × ctVal` = `oiCcy`; WLD `ctVal` = 1) | `oiUsd` alone | — |
| `bybit.linear.open-interest` | `singleOpenInterest` | Base units, single-sided | `openInterest` = both sides counted (ratio 2.000 on BTCUSDT, ETHUSDT, WLDUSDT, 16 Oct 2026) | — |
| `hyperliquid.perp.ctx` | `openInterest` | Coin units; sidedness not published by Hyperliquid | Summing with other venues | Per-venue % change only (a sidedness factor cancels in a ratio) |

Funding:

| `metric_key` | Settled value | Current estimate | Interval | Evidence |
|---|---|---|---|---|
| Binance USDⓈ-M | `/fapi/v1/fundingRate` → `fundingRate` at `fundingTime` | `/fapi/v1/premiumIndex` → `lastFundingRate` (+ `nextFundingTime`); WS `r` | `fundingIntervalHours` from `/fapi/v1/fundingInfo`, else 8 h | — |
| OKX SWAP | `settFundingRate` (`settState`), history `fundingRate` | `fundingRate` (`method: current_period`) applying at `fundingTime` | spacing of `prevFundingTime` → `fundingTime` (8 h on WLD) | — |
| Bybit linear | `/v5/market/funding/history` → `fundingRate` | tickers `fundingRate` (+ `nextFundingTime`) | `fundingIntervalHour` (8 on WLD) | — |
| Hyperliquid | `fundingHistory` | `metaAndAssetCtxs` → `funding` | 1 h | WLD 16 Oct 2026: 0.0000125 per hour = 0.0100% per 8 h, equal to Binance, OKX and Bybit |

Liquidations:

| Feed | Coverage class |
|---|---|
| Binance `{s}@forceOrder` | SAMPLED — at most the latest liquidation per symbol per 1,000 ms |
| Bybit `allLiquidation.{SYMBOL}` | COMPLETE — Bybit documentation: "push all liquidations that occur on Bybit" (USDT, USDC and inverse contracts, every 500 ms) |
| OKX `liquidation-orders` | SAMPLED — OKX documentation: "This data doesn't represent the total number of liquidations on OKX" |
| Hyperliquid | NOT_SUPPORTED (no public stream) |

Tank always reads venue fields by this table; there is no runtime check of their meaning. When a venue changes a field, this table and `semantics.yaml` are updated.


### 2.11 Registry — initial content (`registry.yaml`)

Token identities were verified on 16 Oct 2026 by matching the CoinGecko price with the venue price (LIT 3.17 vs OKX 3.1733; XPL 0.0832 vs 0.0834; WLD 0.516 vs 0.519). Project accounts come from each project's website, CoinGecko or DefiLlama and exist on X (oEmbed HTTP 200).

| `token_id` | Identity (chain: contract) | CoinGecko id | Unlock dataset slug | Organization X accounts (01 §4.3) |
|---|---|---|---|---|
| `bitcoin:BTC` | native | `bitcoin` | — | @bitcoincoreorg |
| `ethereum:ETH` | native | `ethereum` | — | @ethereum, @ethereumfndn |
| `uniswap:UNI` | Ethereum `0x1f9840a85d5af5bf1d1762f925bdaddc4201f984` | `uniswap` | `uniswap` | @Uniswap, @UniswapFND, @unichain |
| `pancakeswap:CAKE` | BNB Chain `0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82` | `pancakeswap-token` | `pancakeswap` | @PancakeSwap |
| `aerodrome:AERO` | Base `0x940181a94a35a4569e4529a3cdfb74e38fd98631` | `aerodrome-finance` | `aerodrome` | @aeroxyz, @dromoslabs, @VelodromeFi |
| `curve:CRV` | Ethereum `0xd533a949740bb3306d119cc777fa900ba034cd52` | `curve-dao-token` | `curve-finance` | @CurveFinance |
| `1inch:1INCH` | Ethereum `0x111111111117dc0aa78b770fa6a738034120c302` | `1inch` | — (none published) | @1inch |
| `0x:ZRX` | Ethereum `0xe41d2489571d322189246dafa5ebde1f4699f498` | `0x` | — (none published) | @0xProject, @zeroexprotocol, @matchaxyz |
| `cow:COW` | Ethereum `0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab` | `cow-protocol` | `cowswap` | @CoWSwap |
| `hyperliquid:HYPE` | Hyperliquid L1, HyperCore token `0x0d01dc56dcaaca66ad901c959b4011ec` | `hyperliquid` | `hyperliquid` | @HyperliquidX, @HyperFND |
| `lighter:LIT` | Ethereum `0x232ce3bd40fcd6f80f3d55a522d03f25df784ee2` | `lighter` | `lighter` | @Lighter_xyz |
| `aave:AAVE` | Ethereum `0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9` | `aave` | `aave` | @aave |
| `morpho:MORPHO` | Ethereum `0x58d97b57bb95320f9a05dc918aef65434969c2b2` | `morpho` | `morpho` | @Morpho |
| `euler:EUL` | Ethereum `0xd9fcd98c322942075a5c3860693e9f4f03aae07b` | `euler` | `euler` | @eulerfinance |
| `ondo:ONDO` | Ethereum `0xfaba6f8e4a5e8ab82f62fe7c39859fa577269be3` | `ondo-finance` | `ondo-finance` | @Ondo, @OndoFoundation, @OndoPerps |
| `layerzero:ZRO` | Ethereum `0x6985884c4392d348587b19cb9eaaf157f13271cd` (same address on Base, Arbitrum, Optimism, BNB Chain, Polygon, Avalanche) | `layerzero` | `layerzero` | @LayerZero_Core, @LayerZero_Labs, @LayerZero_Fndn, @StargateFinance |
| `quant:QNT` | Ethereum `0x4a220e6096b25eadb88358cb44068a3248254675` | `quant-network` | — (none published) | @quantnetwork |
| `avalanche:AVAX` | native (C-Chain) | `avalanche-2` | `avalanche` | @avax, @AvaLabs, @AvalancheFDN |
| `near:NEAR` | native | `near` | `near-protocol` | @NEARProtocol, @near_ai |
| `plasma:XPL` | native (Plasma chain) | `plasma` | `plasma` | @Plasma |
| `ethena:ENA` | Ethereum `0x57e114b691db790c35207b2e685d4a43181e6061` | `ethena` | `ethena` | @ethena |
| `world:WLD` | Ethereum `0x163f8c2467924be0ae7b5347228cabf260318753`; World Chain `0x2cfc85d8e48f8eab294be644d9e25c3030863003`; Optimism `0xdc6ff44d5d932cbd77b52e5612ba0529dc6226f1` | `worldcoin-wld` | `worldcoin` | @worldnetwork, @tfh_technology, @worldcoinfnd, @world_chain_ |

Instrument symbols: Binance `{T}USDT` (spot and USDⓈ-M), Coinbase `{T}-USD`, Kraken `{T}/USD` (BTC is `XBT/USD`), OKX `{T}-USDT` and `{T}-USDT-SWAP`, Bybit `{T}USDT` (spot and linear), Hyperliquid `{T}`. Only markets marked Y in 02 §4.10 are subscribed. Binance spot `LITUSDT` returns 2025 history of Litentry, a different and delisted token, and is never used.

Aliases used to resolve entity names to the registry (V3; whole words, case-insensitive; `$TICKER` always resolves; generic words need the qualifier shown):

| Token | Aliases |
|---|---|
| UNI | Uniswap, Uniswap Labs, Uniswap Foundation, Unichain |
| CAKE | PancakeSwap |
| AERO | Aerodrome |
| CRV | Curve Finance, Curve DAO, crvUSD |
| 1INCH | 1inch, 1inch Network |
| ZRX | 0x Protocol, 0x Labs, Matcha |
| COW | CoW Protocol, CoW Swap, CoW DAO |
| HYPE | Hyperliquid, HyperEVM, HyperCore |
| LIT | Lighter only with "LIT", "Lighter.xyz", "Lighter DEX" or "Lighter exchange" |
| AAVE | Aave, Aave Labs, Aave DAO, GHO |
| MORPHO | Morpho, Morpho Blue |
| EUL | Euler Finance, Euler v2 |
| ONDO | Ondo, Ondo Finance, Ondo Global Markets, USDY, OUSG |
| ZRO | LayerZero, LayerZero Labs |
| QNT | Quant Network, Overledger |
| AVAX | Ava Labs, Avalanche network, Avalanche C-Chain |
| NEAR | NEAR Protocol, NEAR Foundation |
| XPL | Plasma only with "XPL", "Plasma chain" or "Plasma network" |
| ENA | Ethena, Ethena Labs, USDe, sUSDe |
| WLD | Worldcoin, World Network, World App, World Chain, World ID, Tools for Humanity |

### 2.12 Dependency map — initial content (`dependency_map.yaml`)

Seed for the beneficiary walk (§4.7). An edge supported only by this map starts at COMPATIBLE; higher states need quoted evidence.

| Entity | Token | Layers served | Competes with |
|---|---|---|---|
| Uniswap | UNI | VENUE_DEX, LIQUIDITY_EXECUTION | PancakeSwap, Aerodrome, Curve |
| PancakeSwap | CAKE | VENUE_DEX (BNB Chain) | Uniswap, Aerodrome |
| Aerodrome | AERO | VENUE_DEX (Base) | Uniswap, PancakeSwap |
| Curve | CRV | VENUE_DEX (stable and pegged assets) | Uniswap |
| 1inch | 1INCH | LIQUIDITY_EXECUTION (aggregation) | 0x, CoW Protocol |
| 0x | ZRX | LIQUIDITY_EXECUTION (aggregation API) | 1inch, CoW Protocol |
| CoW Protocol | COW | LIQUIDITY_EXECUTION (batch auctions, solvers) | 1inch, 0x |
| Hyperliquid | HYPE | VENUE_DEX (perps, spot), SETTLEMENT_L1_L2 (HyperEVM) | Lighter; centralized perp venues |
| Lighter | LIT | VENUE_DEX (perps) | Hyperliquid |
| Aave | AAVE | LENDING_COLLATERAL | Morpho, Euler |
| Morpho | MORPHO | LENDING_COLLATERAL | Aave, Euler |
| Euler | EUL | LENDING_COLLATERAL | Aave, Morpho |
| Ondo | ONDO | ISSUER_PRODUCT (tokenized treasuries and stocks) | BlackRock, Franklin Templeton |
| LayerZero | ZRO | INTEROPERABILITY | Quant |
| Quant | QNT | INTEROPERABILITY (bank and enterprise networks) | LayerZero |
| Avalanche | AVAX | SETTLEMENT_L1_L2 | NEAR, Plasma |
| NEAR | NEAR | SETTLEMENT_L1_L2 | Avalanche, Plasma |
| Plasma | XPL | SETTLEMENT_L1_L2 (stablecoin payments chain) | Avalanche, NEAR |
| Ethena | ENA | ISSUER_PRODUCT (USDe synthetic dollar) | Circle, Tether, Paxos |
| World | WLD | ISSUER_PRODUCT (identity, World App payments) | — |
| BlackRock | — | ISSUER_PRODUCT (tokenized funds) | Franklin Templeton, Ondo |
| Franklin Templeton | — | ISSUER_PRODUCT (tokenized funds) | BlackRock, Ondo |
| Securitize | — | ISSUER_PRODUCT, CUSTODY_COMPLIANCE (transfer agent) | — |
| Circle | — | ISSUER_PRODUCT (USDC, EURC) | Tether, Paxos, Ethena |
| Tether | — | ISSUER_PRODUCT (USDT) | Circle, Paxos, Ethena |
| Paxos | — | ISSUER_PRODUCT (stablecoin issuance), CUSTODY_COMPLIANCE | Circle, Tether |
| DTCC | — | CUSTODY_COMPLIANCE (clearing, settlement) | — |
| The Clearing House | — | SETTLEMENT_L1_L2 (bank payment networks); Quant partner (24 Sep 2026) | — |
| BNY | — | CUSTODY_COMPLIANCE | Anchorage Digital |
| Anchorage Digital | — | CUSTODY_COMPLIANCE | BNY |
| Binance, Coinbase, OKX, Bybit, Kraken, Robinhood | — | VENUE_DEX (centralized venues) | each other |
| Chainlink | (LINK, outside the universe) | ORACLE_DATA | — |

---


# Source and market APIs, shared limits
 (S01–S34)

Common to all HTTP sources: header `User-Agent: TankV0/1.0 (<operator contact e-mail>)`; conditional GET (`If-None-Match` / `If-Modified-Since`) where the server returns `ETag` / `Last-Modified`; timeout 20 s; 3 retries with 2/4/8 s backoff; escalated interval 15 s (01 FR-D-02). Each payload is stored raw with SHA-256 before parsing; items are deduplicated by the item key in the tables below.

HTML page sources: Tank collects every link on the page that matches the source's article pattern below, resolves it to an absolute URL, and treats each URL not seen before as a new item; the article page is then fetched for its full text. Zero matches on a successful fetch marks the source DEGRADED (layout change). Every source works from plain HTTP; Tank runs no browser. S19 reads the JSON search behind its page.

### 3.1 Regulators and market structure (CORE)

| ID | Endpoint(s) | Format | Item key | Fields parsed | Interval |
|---|---|---|---|---|---|
| S01 | `https://www.sec.gov/news/pressreleases.rss` · `https://www.sec.gov/news/speeches-statements.rss` | RSS | `guid` | title, link, pubDate, description; full text fetched from `link` | 60 s · 120 s |
| S02 | `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=S-1&owner=include&count=40&output=atom` | Atom | entry `id` (accession) | title, updated, link, summary; kept only when the filer name or title matches the registry, or contains "Trust"/"ETF" together with a crypto term | 120 s |
| S03 | `https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]={term}&order=newest&per_page=20` — one request per term: `crypto`, `"digital asset"`, `bitcoin`, `ether`, `stablecoin`, `tokenized` | JSON | `document_number` | title, type (Rule / Proposed Rule / Notice), abstract, html_url, pdf_url, publication_date, agencies, excerpts | 300 s |
| S04 | `https://www.cftc.gov/RSS/RSSGP/rssgp.xml` · `https://www.cftc.gov/RSS/RSSENF/rssenf.xml` | RSS | `guid` | title, link, pubDate | 120 s · 300 s |
| S05 | `https://www.federalreserve.gov/feeds/press_all.xml` | RSS | `guid` | title, link, pubDate, description | 120 s |
| S06 | `https://www.occ.gov/rss/occ_news.xml` | RSS | `guid` | title, link, pubDate | 300 s |
| S07 | `https://home.treasury.gov/news/press-releases` (response ≈ 8 s) · `https://ofac.treasury.gov/rss.xml` (≈ 11 s) | HTML page · RSS | article URL · `guid` | Article pattern `^/news/press-releases/[a-z]{2}\d{4}/?$` (e.g. `/news/press-releases/sb0661`) · title, link, pubDate, description | 300 s |
| S08 | `https://www.dtcc.com/press-releases` (`/news` now redirects to Insights; checked 19 Oct 2026) | HTML page | article URL | Article pattern `https://www.dtcc.com/press-releases/\d{4}/[a-z0-9-]+` (e.g. `/press-releases/2026/dtcc-turns-tokenization-into-reality`) | 300 s |

### 3.2 Exchanges and brokers (CORE)

| ID | Endpoint(s) | Format | Item key | Fields parsed | Interval |
|---|---|---|---|---|---|
| S09 | List: `https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId={48,161,49}&pageNo=1&pageSize=20` (48 New Cryptocurrency Listing, 161 Delisting, 49 Latest Binance News). Detail: `https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode={code}` | JSON | `code` | list: `id`, `code`, `title`, `releaseDate` (ms); detail: `title`, `body`, `publishDate`, catalog names | 30 s |
| S10 | `https://www.okx.com/api/v5/support/announcements?annType={type}` for `announcements-new-listings`, `announcements-delistings`, `announcements-trading-updates`, `latest-events` | JSON | `url` + `pTime` | `annType`, `title`, `url`, `pTime`, `businessPTime` (ms) | 60 s |
| S11 | `https://api.bybit.com/v5/announcements/index?locale=en-US&type={type}&limit=20` for `new_crypto`, `delistings`, `latest_bybit_news` | JSON | `url` | `title`, `description`, `type.key`, `tags`, `url`, `dateTimestamp`, `publishTime` (ms) | 60 s |
| S12 | `https://blog.kraken.com/feed` | RSS | `guid` | title, link, pubDate, categories, content | 120 s |
| S13 | `https://robinhood.com/newsroom/` (moved from `newsroom.aboutrobinhood.com`; checked 19 Oct 2026) | HTML page | article URL | Article pattern `^/(?:us/en/)?newsroom/(?!category/)[A-Za-z0-9-]+/?$` (e.g. `/us/en/newsroom/hood-summit-2026`) | 300 s |
| S14 | Binance `https://api.binance.com/api/v3/exchangeInfo` + `https://fapi.binance.com/fapi/v1/exchangeInfo`; Coinbase `https://api.exchange.coinbase.com/products`; Kraken `https://api.kraken.com/0/public/AssetPairs`; OKX `https://www.okx.com/api/v5/public/instruments?instType=SPOT` and `instType=SWAP`; Bybit `https://api.bybit.com/v5/market/instruments-info?category=spot` and `category=linear` (`limit=1000`); Hyperliquid `POST https://api.hyperliquid.xyz/info` body `{"type":"meta"}` | JSON | venue + symbol + status | symbol, base, quote, status / state, contract type; diff against the previous snapshot | 60 s (Kraken 120 s) |

### 3.3 Protocols and issuers (CORE)

| ID | Page | Article pattern (example match on 16 Oct 2026) | Interval |
|---|---|---|---|
| S15 | `https://blog.uniswap.org/` | `^/(?!category/)[a-z0-9-]{8,}$` (`/uniswap-is-live-on-arc`) | 600 s |
| S16 | `https://ondo.finance/blog` | `^/blog/[a-z0-9-]+$` (`/blog/introducing-ondo-private-markets`) | 600 s |
| S17 | `https://layerzero.network/blog` | `^/blog/[a-z0-9-]+$` (`/blog/introducing-atlas`) | 600 s |
| S18 | `https://www.blackrock.com/corporate/newsroom/media/press-releases` | `/corporate/newsroom/media/press-releases/[a-z0-9-]+` (`…/blackrock-reports-third-quarter-2026`) | 600 s |
| S19 | `POST https://www.franklintempleton.com/api/articleApi` (form: `articleType=press-release`, filter `pressReleaseCategory.exact=news-room`, `en-US`, newest 24) — the JSON search the press-releases page uses; each hit carries title, URL, reference date and full text (`contentString`) | press release URL | 600 s |
| S20 | `https://www.circle.com/pressroom` | `^/pressroom/[a-z0-9-]+$` (`/pressroom/circle-to-announce-q3-2026-financial-results-on-november-4-2026`) | 600 s |
| S21 | `https://tether.io/feed/` — RSS, item key `guid` | — | 300 s |
| S22 | `https://www.paxos.com/newsroom` | `^/newsroom/[a-z0-9-]+$` (`/newsroom/paxos-joins-solana-developer-platform`) | 600 s |

### 3.4 Governance (CORE)

| ID | Endpoint(s) | Format | Item key | Fields parsed | Interval |
|---|---|---|---|---|---|
| S23 | `https://gov.uniswap.org/latest.rss`, `https://governance.aave.com/latest.rss`, `https://gov.ethenafoundation.com/latest.rss`, `https://forum.cow.fi/latest.rss`, `https://forum.morpho.org/latest.rss`, `https://forum.euler.finance/latest.rss`, `https://gov.1inch.network/latest.rss` (moved from `gov.1inch.io`), `https://gov.near.org/latest.rss` | RSS (Discourse) | topic URL | title, link, pubDate, category, first-post text; only topics not seen before | 300 s (Euler, 1inch, NEAR: 600 s) |
| S24 | `POST https://hub.snapshot.org/graphql` — `proposals(first: 20, where: {space_in: ["uniswapgovernance.eth","aavedao.eth","cakevote.eth","cow.eth","morpho.eth","1inch.eth"]}, orderBy: "created", orderDirection: desc) { id title body state start end scores scores_total space { id } }` | GraphQL | `id` (+ `state`) | new proposals; state changes (`active` → `closed`) and final scores become STATE_UPDATE items; `end` schedules a milestone check | 120 s |

### 3.5 Technical artifacts (SECONDARY)

| ID | Endpoint(s) | Format | Item key | Interval |
|---|---|---|---|---|
| S25 | Releases: `https://github.com/Uniswap/v4-core/releases.atom`, `https://github.com/aave-dao/aave-v3-origin/releases.atom`, `https://github.com/cowprotocol/services/releases.atom`, `https://github.com/hyperliquid-dex/hyperliquid-python-sdk/releases.atom`, `https://github.com/1inch/swap-vm/releases.atom`, `https://github.com/elliottech/lighter-python/releases.atom`, `https://github.com/ava-labs/avalanchego/releases.atom`, `https://github.com/near/nearcore/releases.atom`, `https://github.com/worldcoin/world-chain/releases.atom`, `https://github.com/PlasmaLaboratories/node-templates/releases.atom`. Commits: `https://github.com/LayerZero-Labs/LayerZero-v2/commits/main.atom`, `https://github.com/morpho-org/morpho-blue/commits/main.atom` | Atom | entry `id` | 600 s |

Feeds checked on 16 Oct 2026: every release feed above except Uniswap v4-core (last release Jan 2025, kept for the next protocol version) had releases in 2026; Morpho Blue commits are active (latest 16 Oct 2026), LayerZero v2 commits are quiet (latest Feb 2026). Repositories checked and left out because they had no release in 2026: PancakeSwap `infinity-core`, Aerodrome `slipstream` and `contracts`, Curve `curve-stablecoin`, 0x `0x-settler`, Euler `evk-periphery` and `euler-vault-kit`, Ondo `rwa-contracts`, Ethena `ethena-minting-client`; Quant's public repositories are forks and tools.

GitHub evidence is SOURCE_SIDE_SUPPORT: it can raise an integration claim to BUILT at most (03 §3.6).

### 3.6 Institutional and custody (SECONDARY)

| ID | Page | Article pattern (example match on 16 Oct 2026) | Interval |
|---|---|---|---|
| S26 | `https://www.bny.com/corporate/global/en/about-us/newsroom.html` | `/corporate/global/en/about-us/newsroom/press-release/[a-z0-9-]+\.html` (`…/bny-reports-third-quarter-2026-results-130478.html`) | 600 s |
| S27 | `https://www.theclearinghouse.org/About/Media/press-releases` | `/payment-systems/Articles/\d{4}/\d{2}/[A-Za-z0-9-]+` (`/payment-systems/Articles/2026/09/The-Clearing-House-Partners-with-Quant-to-Advance-the--On-Chain-Money-Initiative`) | 600 s |
| S28 | `https://www.anchorage.com/insights` | `^/press-room/[a-z0-9-]+$` (`/press-room/the-lifecycle-of-staked-eth`) | 600 s |

### 3.7 X (S29, SECONDARY)

- Account set: the 86 handles in 01 §4.3, listed as `x_accounts` in `registry.yaml`.
- Reader: one synchronous run of the Apify actor `fdfsfsdfasfa/tank-x-notifier` (build `direct`) every 120 s, authorized by `APIFY_API_TOKEN`; no X account or session. The actor reads each account's public profile page signed out and returns every visible own post, usually the latest five per account (no reposts, no protected accounts), with the next per-account cursors.
- Per post: status ID, author handle, complete text, exact publish time and post URL; the raw evidence is the actor response. A post that quotes, replies to or links another post carries that post in the item's `quoted` field, one block per post: `@monitored quoted|replied to|linked a post by @author (post URL, published time):` followed by its text, or `[text unavailable: reason]`. That text is the other author's words, not the monitored account's. Signed-out profiles show no reposts and no replies to other accounts, so those never arrive. The first run for an account only records its cursor, so posts from before Tank watched it are never ingested; later, a post is new when its ID is above the cursor. An account that posted more than its profile shows between runs is reported as a history gap for that pass.
- Handle check: `tank sources check` calls `https://publish.twitter.com/oembed?url=https://x.com/{handle}` for each handle (follows the redirect to `publish.x.com`; HTTP 200 = exists) and confirms the owner from the profile bio (`https://syndication.twitter.com/srv/timeline-profile/screen-name/{handle}`, rate-limited to a few requests per minute) or from the person's IQ.wiki, Wikidata or GitHub profile; all 86 passed on 16–17 Oct 2026 (e.g. @el33th4xor bio: "Founder and CEO @avalabs"; @vnovakovski: IQ.wiki "founder and CEO of Lighter"; @fleupold_: Devcon speaker page and GitHub bio "Technical lead at CowProtocol"). Organization handles were taken from the project's own website, docs or GitHub organization; people's handles from the official team page, IQ.wiki, Wikidata, GitHub or reputable news. Handles that must not be used: wrong person — @nathanallman (an unrelated account; Ondo's founder Nathan Allman used @nathanlallman and died on 25 May 2026); unconfirmed or squatted — @fleupold (Felix Leupold is @fleupold_), @CoWDAO, @HyperFoundation, @HyperliquidLabs, @worldcoin (re-registered in Mar 2026); HTTP 404 — @aerodromefi, @OndoFinance, @quant_network, @ethena_labs, @eulerlabs. Left out because no official source confirms the owner: @iliensinc (Hyperliquid co-founder), @MathisGD_ (Morpho co-founder), @chef_mochi (PancakeSwap). Left out as dormant or no longer at the project: @nearfoundation, @k06a, @PaoloTasca, @kevinsekniqi, @Tederminant, @John1wu.
- Terms: the actor reads the public X web UI, which carries terms-of-service risk, and Apify bills each run (recorded in `terms_notes`).

### 3.8 News (SECONDARY)

| ID | Endpoint(s) | Interval |
|---|---|---|
| S30 | `https://feeds.bbci.co.uk/news/business/rss.xml` · `https://feeds.bbci.co.uk/news/technology/rss.xml` | 120 s · 300 s |
| S31 | `https://feeds.bloomberg.com/markets/news.rss` | 120 s |
| S32 | `https://news.google.com/rss/search?q=site:reuters.com+crypto&hl=en-US&gl=US&ceid=US:en` | 300 s |
| S33 | `https://www.coindesk.com/arc/outboundfeeds/rss/` | 120 s |
| S34 | `https://www.theblock.co/rss.xml` | 120 s |

All RSS; item key `guid` (else `link`); fields title, link, pubDate, description. Bloomberg headlines (paywalled) are used for discovery; facts are confirmed from primary sources.

---

## 4. Market data catalog

**The sandbox never connects to an exchange.** tank-capture is the only client of the exchange hosts below; it records every print, trades, books, klines, OI, funding, liquidations and tickers into ClickHouse database `mkt` (see Executable sandbox access), which the agent reads. The endpoints are listed so the agent knows what each column came from, not to call them.

Tank-side request budgets are set at ≤ 50% of each venue's documented limit. Binance limits below were read from `exchangeInfo.rateLimits`. Instrument symbols per venue are listed in 03 §2.11.

### 4.1 Binance Spot

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://data-stream.binance.vision/stream?streams={s}@aggTrade` | `a` agg ID, `p`, `q`, `f`/`l` first/last trade ID, `T` ms, `m` buyer-is-maker (`true` → aggressive SELL) |
| Hot book | WSS `{s}@depth@100ms` + snapshot `GET https://api.binance.com/api/v3/depth?symbol={S}&limit=1000` (sync by `lastUpdateId`) | bids/asks |
| Base klines | `GET https://api.binance.com/api/v3/klines?symbol={S}&interval=1m` | row: openTime, o, h, l, c, volume, closeTime, quoteVolume, trades, takerBuyBase, takerBuyQuote |
| Backfill | `GET /api/v3/aggTrades?symbol={S}&fromId=` or `startTime`/`endTime` (≤ 1 h window) | as WS |
| 24h volume | `GET /api/v3/ticker/24hr?symbol={S}` | volume, quoteVolume |
| Limits | 6,000 request weight / min / IP; Tank budget ≤ 3,000; header `X-MBX-USED-WEIGHT-1M` read on every response | — |
| Exclusion | `LITUSDT` spot history belongs to Litentry (delisted, different token): never used for LIT | — |

### 4.2 Binance USDⓈ-M Futures

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://fstream.binance.com/market/stream?streams={s}@aggTrade` — the market path; the old `/stream` and `/ws` paths accept connections but push no market data (checked 17 Oct 2026) | as spot |
| Order book | WSS `wss://fstream.binance.com/public/stream?streams={s}@depth@100ms`; initialize with `GET https://fapi.binance.com/fapi/v1/depth?symbol={S}&limit=1000` | `U`, `u`, `pu` sequence bridge; `b`, `a` price/quantity updates. Keep this public connection separate from the market connection; a book disconnect does not establish a trade gap. |
| Mark / funding estimate | WSS `{s}@markPrice@1s` on the same `/market/stream` connection | `p` mark, `i` index, `r` current funding estimate, `T` next funding time |
| Liquidations | WSS `{s}@forceOrder` on the same `/market/stream` connection | `o.S` side, `o.q` qty, `o.ap` avg price, `o.T` — **SAMPLED**: at most the latest liquidation per symbol per 1,000 ms |
| OI now | `GET https://fapi.binance.com/fapi/v1/openInterest?symbol={S}` | `openInterest` (base units, single-sided), `time` |
| OI history | `GET /futures/data/openInterestHist?symbol={S}&period=5m` | `sumOpenInterest` (base), `sumOpenInterestValue` (USD), `timestamp`; last 30 days only |

Binance historical OI `timestamp` is the **end** of its requested period. Keep the raw timestamp and actual period duration; normalized anchor `END` makes a five-minute bucket cover the preceding five minutes. Bybit historical OI is a timestamped stock observation with `EXACT` precision (`SOURCE_TIMESTAMP_POINT`), not a bucket whose edge is inferred from request cadence. Request cadence remains collection provenance. The field registry supplies these meanings without runtime semantic re-verification. The Binance documentation receipt hashes its quoted field description only; direct local retrieval returned HTTP 202 with an empty body, so no full-page capture is claimed.
| Funding estimate | `GET /fapi/v1/premiumIndex?symbol={S}` | `markPrice`, `indexPrice`, `lastFundingRate` (current estimate), `nextFundingTime`, `interestRate` |
| Funding settled | `GET /fapi/v1/fundingRate?symbol={S}` | `fundingTime`, `fundingRate`, `markPrice` |
| Funding interval | `GET /fapi/v1/fundingInfo` | `fundingIntervalHours` for symbols with non-default interval (default 8 h) |
| Taker volume | `GET /futures/data/takerlongshortRatio?symbol={S}&period=5m` | `buyVol`, `sellVol`, `buySellRatio` |
| Limits | 2,400 request weight / min / IP; Tank budget ≤ 1,200 | — |

The separate futures depth route follows Binance's [WebSocket change notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice) and was checked with actual BTC/ETH depth streams on 18 October 2026. Preserve snapshot and stream sequence checks when reconnecting.

### 4.3 Coinbase Exchange

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://ws-feed.exchange.coinbase.com` channel `matches` | `trade_id`, `side` = **maker** side (aggressor is the opposite), `size`, `price`, `time` |
| Hot book | same socket, channel `level2_batch`; `heartbeat` for sequence checks | — |
| Backfill | `GET https://api.exchange.coinbase.com/products/{id}/trades` (paginate with `after`) | `trade_id`, `side` (maker), `size`, `price`, `time` |
| Book snapshot | `GET /products/{id}/book?level=2` | — |
| 24h volume | `GET /products/{id}/stats` | — |
| Limits | Tank budget ≤ 5 requests/s | — |

### 4.4 OKX

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://ws.okx.com:8443/ws/v5/public` channel `trades` | `tradeId`, `px`, `sz`, `side` (taker), `ts`, `count` (pushes may aggregate one taker order) |
| Hot book | channel `books` (400 levels, CRC32 checksum) | — |
| OI | channel `open-interest`; `GET https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId={X}-USDT-SWAP` | `oi` (contracts), `oiCcy` (coin), `oiUsd`, `ts` |
| Contract size | `GET /api/v5/public/instruments?instType=SWAP&instId=` | `ctVal`, `ctMult`, `ctValCcy` (WLD-USDT-SWAP: `ctVal` = 1 WLD) |
| Funding | channel `funding-rate`; `GET /api/v5/public/funding-rate?instId=` | `fundingRate` (current period), `settFundingRate` (last settled), `settState`, `fundingTime`, `nextFundingTime`, `prevFundingTime`, `premium` |
| Funding history | `GET /api/v5/public/funding-rate-history?instId=` | `fundingRate`, `realizedRate`, `fundingTime` |
| Mark / index | channels `mark-price`, `index-tickers` | — |
| Liquidations | channel `liquidation-orders` (`instType` SWAP) | **SAMPLED** — OKX documentation: "This data doesn't represent the total number of liquidations on OKX" |
| Backfill | `GET /api/v5/market/history-trades?instId=` | as WS |

### 4.5 Bybit

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://stream.bybit.com/v5/public/spot` and `/v5/public/linear`, topic `publicTrade.{SYMBOL}` | `i` trade ID, `T` ms, `S` taker side, `v`, `p`, `BT` block trade |
| Hot book | topic `orderbook.200.{SYMBOL}` (spot) | — |
| Linear ticker | topic `tickers.{SYMBOL}`; `GET https://api.bybit.com/v5/market/tickers?category=linear&symbol=` | `openInterest` (**double-sided**), `openInterestValue`, `fundingRate`, `nextFundingTime`, `fundingIntervalHour`, `markPrice`, `indexPrice` |
| OI | `GET /v5/market/open-interest?category=linear&symbol={S}&intervalTime=5min` | `openInterest` and `singleOpenInterest`; observed 16 Oct 2026: `openInterest` / `singleOpenInterest` = 2.000 for BTCUSDT, ETHUSDT, WLDUSDT → Tank uses `singleOpenInterest` |
| Funding settled | `GET /v5/market/funding/history?category=linear&symbol=` | `fundingRate`, `fundingRateTimestamp` |
| Liquidations | topic `allLiquidation.{SYMBOL}` | **COMPLETE** — Bybit documentation: "push all liquidations that occur on Bybit", USDT/USDC/inverse contracts, every 500 ms |
| Backfill | `GET /v5/market/recent-trade?category=spot&symbol=` | `execId`, `price`, `size`, `side`, `time`, `seq`; last 1,000 trades only |

### 4.6 Kraken

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://ws.kraken.com/v2` channel `trade` | `trade_id`, `side` (taker), `qty`, `price`, `timestamp` |
| Hot book | channel `book` (depth 1000, CRC32 checksum) | — |
| Backfill | `GET https://api.kraken.com/0/public/Trades?pair={P}&since=` | row: price, volume, time (seconds, float), `b`/`s` (taker side), `l`/`m`, misc, trade_id |
| Limits | Tank budget ≤ 1 request/s | — |

### 4.7 Hyperliquid (perpetuals)

| Purpose | Endpoint | Key fields |
|---|---|---|
| Hot trades | WSS `wss://api.hyperliquid.xyz/ws` subscription `trades` | `coin`, `side` (`B` = aggressive buy, `A` = aggressive sell — measured 16 Oct 2026 on BTC, ETH, HYPE, SOL: 90% of `B` trades printed at the ask, 79% of `A` trades at the bid), `px`, `sz`, `time`, `tid` |
| Context | subscription `activeAssetCtx`; `POST https://api.hyperliquid.xyz/info` `{"type":"metaAndAssetCtxs"}` | `funding` (hourly), `openInterest` (coin units; sidedness not published → used only as per-venue % change, never summed), `markPx`, `oraclePx`, `premium`, `dayNtlVlm` |
| Funding history | `{"type":"fundingHistory","coin":"{X}","startTime":…}` | — |
| Liquidations | no public stream | NOT_SUPPORTED |

### 4.8 History, fundamentals, supply

| Source | Endpoint | Use |
|---|---|---|
| Binance public data archive | `https://data.binance.vision/data/spot/daily/klines/{S}/1m/{S}-1m-{YYYY-MM-DD}.zip`, `…/spot/daily/aggTrades/{S}/{S}-aggTrades-{date}.zip`, `…/futures/um/daily/klines/{S}/1m/…`, `…/futures/um/daily/metrics/{S}/{S}-metrics-{date}.zip` (5m OI), `…/futures/um/monthly/fundingRate/{S}/{S}-fundingRate-{YYYY-MM}.zip`; each with `.CHECKSUM` = `"<sha256>  <file name>"` — downloaded only by tank-capture (`ARCHIVE_FETCH` capture request), checksum-verified and stored at `/data/archive/binance/<path after /data/>`; read the stored file | Pre-event windows older than REST retention, retro reaction checkpoints, fixture replay (03 §5.2) |
| DefiLlama API | `https://api.llama.fi/summary/fees/{protocol}?dataType=dailyFees` / `dailyRevenue` / `dailyHoldersRevenue` (`total24h`, `total30d`, `totalDataChart`); `https://stablecoins.llama.fi/stablecoincharts/{chain}`; `https://yields.llama.fi/lendBorrow` (DefiLlama shuts down the free yield endpoints `/pools` and `/chart/{pool}` on 13 Nov 2026, moving them to the paid API; Tank uses only `/lendBorrow`, which the notice does not name and which still answered on 19 Oct 2026) | Capture chain, stablecoin supply of the settlement chain, lending utilization |
| DefiLlama unlock datasets | `https://defillama-datasets.llama.fi/emissions/{slug}` (list: `…/emissionsProtocolsList`, 371 protocols on 16 Oct 2026); slugs per token in 03 §2.11; none published for 1INCH, ZRX, QNT | `unlocks` for supply context, refreshed daily |
| Exchange wallets | DefiLlama-Adapters `projects/binance/index.js` at commit `4a6ef31a4eed4f24882a7d82bff50306b40b6d6c` (17 EVM addresses) and `projects/okex/index.js` at commit `b70f0fb4dc9ff463866d676c14d21b0ed0930d76` (323 EVM addresses) → `exchange_addresses.yaml` | Exchange inventory and wallet flows (Bybit, Coinbase, Kraken: no public list → n/a) |
| Etherscan API v2 (free key) | `https://api.etherscan.io/v2/api?chainid={id}&module=account&action=tokenbalance&contractaddress={token}&address={wallet}&apikey={key}`; `action=tokentx` for transfers | Balances and transfers of the wallets above for EVM tokens of the universe |

### 4.9 Capture tiers

| Tier | Universe | What runs | Cadence |
|---|---|---|---|
| Base (always) | 22 tokens (01 §4.1) on every covered market (§4.10) | REST 1m klines; 24h tickers; OI and funding | Klines every 60 s; 24h tickers and OI + funding every 300 s |
| Hot (escalated, max 15 tokens) | Tokens in an event's analysis set | WebSocket trades and books on every covered venue; OI + funding every 60 s; liquidations; mark/index | Streaming; book summaries every 10 s; 24h tickers every 300 s |

Load check: base tier = 181 market series (22 tokens over the venues of the coverage matrix in §4.10) → ≈ 181 kline requests/min (Binance: 43 series, ≈ 86 weight/min of a 3,000 budget); hot tier ≤ 15 tokens × 9 markets = 135 WebSocket subscriptions on ≈ 10 connections.

### 4.10 Universe coverage (16 Oct 2026)

Y = active USDT/USDC/USD market for spot, USDT-margined perpetual for perp. Regenerated by `tank coverage` at V0-A and weekly.

| Token | BN spot | CB spot | OKX spot | BY spot | KR spot | BN perp | OKX perp | BY perp | HL perp |
|---|---|---|---|---|---|---|---|---|---|
| BTC, ETH, UNI, ONDO, ZRO, CRV, AAVE, NEAR, HYPE, ENA, MORPHO, AVAX, XPL, WLD | Y | Y | Y | Y | Y | Y | Y | Y | Y |
| ZRX, 1INCH | Y | Y | Y | Y | Y | Y | Y | Y | — |
| AERO | Y | Y | — | Y | Y | Y | Y | Y | Y |
| CAKE | Y | Y | — | Y | Y | Y | — | Y | Y |
| QNT | Y | Y | — | Y | Y | Y | Y | Y | — |
| COW | Y | Y | — | — | Y | Y | — | Y | — |
| EUL | Y | Y | — | — | Y | Y | — | Y | — |
| LIT | — | — | Y | Y | Y | Y | Y | Y | Y |

Every token has at least three spot venues and two perpetual venues. Binance spot `LITUSDT` is excluded (Litentry history, §4.1).

---


# Design table contracts and deployed schema (PostgreSQL 16 and ClickHouse)

The DDL excerpt below records the design document's table vocabulary and field meanings. It is not an executable dump of the deployed database. Versioned migrations define the implementation. Non-market registry/event/operational rows retain JSONB `document` plus typed projections. Nested objects such as EMS `dims`, Market Truth `divergence` and `data_confidence`, the decision `outlook`, the alert `card_struct`, the job `payload` and tool call `input`/`result` are only in `document`: read them as `document->'outlook'`. GIN indexes exist only on `evt.claims`, `ops.agent_sessions`, `ops.tool_calls`, `out.deliveries` and `out.user_questions`, and serve `document @> '{"key": "value"}'`, not `document->>'key' = 'value'`. Market series and prints are typed ClickHouse tables (the section "Market tables in ClickHouse"): no whole-row `document` or `extras`. Their named JSONB fields hold domain structures such as boundary segments, coverage and per-field missing reasons. Inspect the live read-only schema before composing a query; do not assume that an illustrative primary-key declaration is the physical storage key.

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('reg','ingest','evt','mkt','ana','out','ops','scout','qa')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'evt' AND table_name = 'events'
ORDER BY ordinal_position;
```

Market series are in ClickHouse (the section "Market tables in ClickHouse"):

```sql
SELECT instrument_id, metric_key, ts, ts_prec, received_ts, oi_native, oi_usd, oi_usd_reason
FROM mkt.oi_obs FINAL
WHERE instrument_id = {i:String} AND ts <= {cutoff:DateTime64(6, 'UTC')}
ORDER BY ts DESC
LIMIT 20;
```

Every declared timestamp column, in PostgreSQL and in ClickHouse, has nullable `<field>_prec` and `<field>_prec_reason` columns; exactness is never assigned from ISO formatting. A NULL precision means unknown, with its reason (`INCOMPLETE` when the source did not declare it). Read the precision and its reason with the timestamp. Registry clocks remain in JSONB: bootstrap recursively preserves captured `verified_at`, `resolved_at`, `checked_at` and `identity_reviewed_at` values and their declared precision; it never replaces a historical date with the current collection clock.

```sql
SELECT ts, ts_prec, ts_prec_reason,
       received_ts, received_ts_prec, received_ts_prec_reason
FROM mkt.oi_obs FINAL
WHERE instrument_id = {i:String} AND ts <= {cutoff:DateTime64(6, 'UTC')}
ORDER BY ts DESC LIMIT 20;
```

```sql
SELECT published_at, published_prec, published_prec_reason,
       published_at_bucket_size_s, observed_at, observed_at_prec
FROM ingest.source_items
WHERE source_id = %s
ORDER BY observed_at DESC LIMIT 20;

SELECT token_id,
       document #> '{fundamentals_evidence,verified_at}' AS verified_at,
       document #> '{fundamentals_evidence,verified_at_prec}' AS verified_at_prec,
       document #> '{fundamentals_evidence,verified_at_prec_reason}' AS precision_reason
FROM reg.tokens WHERE token_id = %s;
```

Source publication is the compatibility exception: SQL `published_prec` retains the existing enum `EXACT`, `BUCKET`, `COLLECTION_ONLY`; bucket duration is explicit in `published_at_bucket_size_s`, while JSON `published_at_precision` carries rich `BUCKET(size)` provenance. A missing publication time can be `COLLECTION_ONLY` with a missing-value reason; it is not the observation time. Other canonical projections use `<field>_prec` (for example `received_ts_prec`) and `<field>_prec_reason`. Never infer occurrence precision from collection time, field formatting, or database microsecond storage.

Use bound parameters for values. Market provenance and null reasons are explicit typed columns or named domain JSONB fields. Missing `buy_usd`, `sell_usd` or USD size buckets require their own standard `null_reasons` entry; valid native quantities and coverage survive missing quote/USD conversion. Never substitute USD zero. Open-interest `ts_prec` and funding `as_of` remain required. Read the actual columns/constraints when a design excerpt and the live projection differ; report the difference rather than inventing a value or altering the schema.

```sql
CREATE SCHEMA reg; CREATE SCHEMA ingest; CREATE SCHEMA evt; CREATE SCHEMA mkt;
CREATE SCHEMA ana; CREATE SCHEMA out; CREATE SCHEMA ops; CREATE SCHEMA scout; CREATE SCHEMA qa;

CREATE TABLE ingest.source_items (
  item_id            text PRIMARY KEY,                 -- ITM-<ulid>
  source_id          text NOT NULL,                    -- S01..S34
  feed               text NOT NULL,                    -- e.g. 'binance:catalog48', 'sec:press', 'x:list'
  external_id        text NOT NULL,                    -- guid / code / document_number / status id / block sha
  url                text,
  title              text,
  body               text,
  published_at       timestamptz,
  published_prec     text NOT NULL CHECK (published_prec IN ('EXACT','BUCKET','COLLECTION_ONLY')),
  observed_at        timestamptz NOT NULL,
  raw_path           text NOT NULL,
  raw_sha256         char(64) NOT NULL,
  content_hash       char(64) NOT NULL,
  source_class       text NOT NULL,
  relevance_decision text NOT NULL CHECK (relevance_decision IN ('ALWAYS','SEND','SKIP')),
  relevance_p        real,
  route_decision     text CHECK (route_decision IN ('NEW_SESSION','ADDED')),
  route_event_id     text,
  route_p            real,
  extraction_status  text NOT NULL DEFAULT 'PENDING',
  UNIQUE (source_id, feed, external_id)
);
CREATE INDEX ON ingest.source_items (content_hash);
CREATE INDEX ON ingest.source_items (observed_at);

CREATE TABLE evt.events (
  event_id           text PRIMARY KEY,                 -- EVT-<ulid>
  statement          text NOT NULL,
  theme_tags         text[] NOT NULL DEFAULT '{}',
  event_time         timestamptz,
  event_time_prec    text,
  public_at          timestamptz NOT NULL,
  public_at_prec     text NOT NULL,
  public_at_basis    text NOT NULL,
  known_to_us_at     timestamptz NOT NULL,
  verified_at        timestamptz,
  freshness          text NOT NULL CHECK (freshness IN ('FRESH','NORMAL','MEDIUM','POOR','OLD','STALE')),
  event_state        text NOT NULL,
  state_verified     boolean NOT NULL,
  negative_marker    text,
  certainty          text NOT NULL,
  timing_confidence  text NOT NULL,
  best_source_class  text NOT NULL,
  lifecycle          text NOT NULL,
  merged_into        text REFERENCES evt.events(event_id),
  current_ems_id     text
);
CREATE INDEX ON evt.events (known_to_us_at);
CREATE INDEX ON evt.events (lifecycle);

CREATE TABLE evt.event_observations (
  obs_id        text PRIMARY KEY,
  event_id      text NOT NULL REFERENCES evt.events(event_id),
  item_id       text NOT NULL REFERENCES ingest.source_items(item_id),
  link_type     text NOT NULL,
  public_at     timestamptz NOT NULL,
  freshness     text NOT NULL,
  match_method  text NOT NULL,
  observed_at   timestamptz NOT NULL
);

CREATE TABLE evt.claims (
  claim_id          text PRIMARY KEY,
  obs_id            text NOT NULL REFERENCES evt.event_observations(obs_id),
  claim_type        text NOT NULL,
  subject           text, predicate text, object text,
  value_num         numeric, value_unit text,
  state_claim       text,
  directionality    text NOT NULL,
  evidence_class    text NOT NULL,
  verified          boolean NOT NULL,
  quote             text NOT NULL,
  accepted          boolean NOT NULL,
  rejection_reason  text
);

CREATE TABLE evt.state_changes (                       -- append-only
  id          bigserial PRIMARY KEY,
  event_id    text NOT NULL REFERENCES evt.events(event_id),
  edge_id     text,
  ladder      text NOT NULL CHECK (ladder IN ('EVENT','INTEGRATION')),
  from_state  text,
  to_state    text NOT NULL,
  evidence    text[] NOT NULL,                         -- claim_ids
  changed_at  timestamptz NOT NULL
);

CREATE TABLE evt.ems_assessments (
  ems_id          text PRIMARY KEY,
  event_id        text NOT NULL REFERENCES evt.events(event_id),
  version         int  NOT NULL,
  cutoff          timestamptz NOT NULL,
  class           text NOT NULL,
  criterion       text NOT NULL,
  class_reasons   text[] NOT NULL,
  config_version  text NOT NULL,
  model_id        text NOT NULL,
  prompt_version  text NOT NULL,
  UNIQUE (event_id, version)
);

CREATE TABLE ana.market_truth (
  mta_id            text PRIMARY KEY,
  token_id          text NOT NULL,
  event_id          text REFERENCES evt.events(event_id),
  cutoff            timestamptz NOT NULL,
  primary_window    text NOT NULL,
  move_class        text NOT NULL,
  support           int,
  alternatives      text[] NOT NULL,                  -- other eligible move classes (§5.8)
  flags             text[] NOT NULL,
  decisive_facts    text[] NOT NULL,
  class_confidence  text NOT NULL,
  config_version    text NOT NULL,
  code_version      text NOT NULL
);
CREATE INDEX ON ana.market_truth (token_id, cutoff);

CREATE TABLE ana.decisions (
  dec_id        text PRIMARY KEY,
  event_id      text NOT NULL REFERENCES evt.events(event_id),
  token_id      text NOT NULL,
  mta_id        text NOT NULL REFERENCES ana.market_truth(mta_id),
  cutoff        timestamptz NOT NULL,
  thesis_grade  char(1) NOT NULL, thesis_rule text NOT NULL,
  trade_grade   char(1) NOT NULL, trade_reasons text[] NOT NULL,
  action        text NOT NULL,
  action_rule   text NOT NULL,                         -- A0..A12, B1..B4
  tactical      boolean NOT NULL,
  investigate   boolean NOT NULL,
  call_id       text,
  conditions    text[] NOT NULL                        -- cond_ids shown on the card (≤ 2)
);

CREATE TABLE ana.calls (
  call_id             text PRIMARY KEY,
  event_id            text NOT NULL REFERENCES evt.events(event_id),
  token_id            text NOT NULL,
  opened_by_alert     text NOT NULL,
  opened_by_version   int  NOT NULL,
  opened_at           timestamptz NOT NULL,
  entry_ref_price     numeric NOT NULL,
  invalidation_price  numeric NOT NULL,
  status              text NOT NULL CHECK (status IN ('OPEN','CLOSED')),
  closed_by           text CHECK (closed_by IN ('EXIT','EXPIRED')),
  closed_at           timestamptz
);

CREATE TABLE out.alerts (                              -- append-only, one row per version
  alert_id            text NOT NULL,
  version             int  NOT NULL,
  kind                text NOT NULL CHECK (kind IN ('PARTIAL','FULL','UPDATE')),
  event_id            text NOT NULL REFERENCES evt.events(event_id),
  generated_at        timestamptz NOT NULL,
  card_text           text NOT NULL,
  decisions           text[] NOT NULL,
  evidence_bundle_id  char(64) NOT NULL,
  supersedes_version  int,
  config_version      text NOT NULL,
  code_version        text NOT NULL,
  model_id            text,
  prompt_version      text,
  PRIMARY KEY (alert_id, version)
);

CREATE TABLE out.ops_notices (                         -- append-only
  notice_id   text PRIMARY KEY,
  kind        text NOT NULL CHECK (kind IN ('ITEM_PROGRESS','ENTRY_ERRORS','ENTRY_ERROR',
                                            'DELIVERY_FAILED','HEARTBEAT')),
  subject_id  text,                                    -- item_id / source_id / alert_id
  text        text NOT NULL,
  created_at  timestamptz NOT NULL,
  created_by  text NOT NULL                            -- ops-progress | source-poller | delivery | scheduler
);

CREATE TABLE out.ops_cards (                           -- mutable: the Ops card of one material
  item_id      text NOT NULL,
  chat_id      text NOT NULL,
  message_ref  text,                                   -- Telegram message edited in place
  UNIQUE (chat_id, item_id)
);

CREATE TABLE out.deliveries (                          -- append-only ledger, one row per transition
  id           bigserial PRIMARY KEY,
  alert_id     text,
  version      int,
  notice_id    text REFERENCES out.ops_notices(notice_id),
  channel      text NOT NULL,                          -- 'telegram:alerts' | 'telegram:ops'
  status       text NOT NULL CHECK (status IN ('GENERATED','SENT','FAILED')),
  message_ref  text,                                   -- Telegram message_id
  attempt      int  NOT NULL,
  at           timestamptz NOT NULL,
  error        text,
  FOREIGN KEY (alert_id, version) REFERENCES out.alerts(alert_id, version),
  CHECK ((alert_id IS NOT NULL AND version IS NOT NULL AND notice_id IS NULL)
      OR (alert_id IS NULL AND version IS NULL AND notice_id IS NOT NULL))
);

CREATE TABLE ops.jobs (
  job_id        text PRIMARY KEY,
  type          text NOT NULL,
  priority      smallint NOT NULL,
  idem_key      text NOT NULL UNIQUE,
  status        text NOT NULL DEFAULT 'QUEUED'
                CHECK (status IN ('QUEUED','RUNNING','DONE','FAILED','EXPIRED')),
  attempts      int NOT NULL DEFAULT 0,
  max_attempts  int NOT NULL,
  run_after     timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  last_error    text
);
CREATE INDEX ON ops.jobs (status, priority, run_after);

-- A column without NOT NULL is null only with its reason in document.missing_fields.<column>;
-- an unknown cost is never stored as 0.
CREATE TABLE ops.llm_usage (
  id                  bigserial PRIMARY KEY,
  at                  timestamptz NOT NULL,
  call                text NOT NULL,                   -- FILTER | ROUTE (Decision-1) | SESSION | SCOUT
  subject_id          text NOT NULL,                   -- item_id / event_id / alert_id / prp_id
  model               text NOT NULL,
  effort              text,                            -- null for Decision-1
  input_tokens        int,
  cache_read_tokens   int,                             -- null for Decision-1
  cache_write_tokens  int,                             -- null for Decision-1
  output_tokens       int,
  cost_usd            numeric(10,6),                   -- null for Decision-1: Azure reports no cost
  web_searches        int NOT NULL DEFAULT 0,
  stop_reason         text NOT NULL
);

CREATE TABLE ops.agent_sessions (
  session_id      text PRIMARY KEY,                    -- OpenMA session id
  event_id        text REFERENCES evt.events(event_id),   -- set by record_event; a closed session's event gets a fresh generation, so earlier closed sessions share it
  opened_by_item  text NOT NULL REFERENCES ingest.source_items(item_id),
  agent_version   text NOT NULL,                       -- tank-event-analyst@<version>
  status          text NOT NULL CHECK (status IN ('OPEN','ENDED','FAILED')),
  opened_at       timestamptz NOT NULL,
  last_item_at    timestamptz NOT NULL,                -- routing window, §3.1
  last_event_seq  bigint NOT NULL DEFAULT 0            -- stream resume point
);

CREATE TABLE ops.tool_calls (
  call_id         text PRIMARY KEY,                    -- OpenMA custom tool_use id
  session_id      text NOT NULL REFERENCES ops.agent_sessions(session_id),
  tool            text NOT NULL,
  status          text NOT NULL CHECK (status IN ('PENDING','DONE','REFUSED','ERROR')),
  requested_at    timestamptz NOT NULL,
  answered_at     timestamptz
);

-- Append-only enforcement
CREATE FUNCTION ops.forbid_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'append-only table: %', TG_TABLE_NAME; END $$;
CREATE TRIGGER alerts_ro        BEFORE UPDATE OR DELETE ON out.alerts        FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TRIGGER deliveries_ro    BEFORE UPDATE OR DELETE ON out.deliveries    FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TRIGGER state_changes_ro BEFORE UPDATE OR DELETE ON evt.state_changes FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
-- same trigger on out.ops_notices, out.evidence_bundles, out.reaction_checkpoints, qa.reviews

-- Read-only role for the agents' sandboxes (02 §8, §11); password: §13; statement timeout and connection limit: §12
CREATE ROLE tank_agent LOGIN;
GRANT USAGE ON SCHEMA reg, ingest, evt, mkt, ana, out, ops, scout, qa TO tank_agent;
GRANT SELECT ON ALL TABLES IN SCHEMA reg, ingest, evt, mkt, ana, out, ops, scout, qa TO tank_agent;
ALTER DEFAULT PRIVILEGES IN SCHEMA reg, ingest, evt, mkt, ana, out, ops, scout, qa GRANT SELECT ON TABLES TO tank_agent;
ALTER ROLE tank_agent SET default_transaction_read_only = on;
-- The sandbox URL TANK_DB_URL carries options=-c search_path=reg,ingest,evt,mkt,ana,out,ops,scout,qa,public: a bare table name resolves

-- Job claim by one of the service's workers (§3.8); on start: UPDATE ops.jobs SET status = 'QUEUED' WHERE status = 'RUNNING';
UPDATE ops.jobs
   SET status = 'RUNNING', attempts = attempts + 1
 WHERE job_id = (SELECT job_id FROM ops.jobs
                  WHERE status = 'QUEUED' AND run_after <= now() AND expires_at > now()
                  ORDER BY priority, run_after
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1)
RETURNING *;
```

Metric meaning and units come from the authoritative field registry, without runtime semantic-confidence or self-test gates. Any `semantic_confidence` columns retained in the physical schema are legacy storage and do not gate interpretation, Data Confidence or decisions.


# Finding record IDs for tool arguments

Tank's messages and tool results are text: they never show record IDs. Tool arguments name records only by ID, never by a description; Tank checks every ID (it exists, is of the right kind and lies in the tool's scope) and a refusal names the argument path and the query below to use.

- **Registry keys** (`token_id`, `entity_id`, `instrument_id`, `source_id`) are shown with every registry object as `Name · key`: `World (WLD) · world:WLD`, `Uniswap · uniswap`, `binance SPOT BTCUSDT · binance:spot:BTCUSDT`, `Venue instrument lists · S14`. Pass the key after ` · `. A key not shown yet is in «Registry keys» below.
- **Record IDs** (`item_id`, `event_id`, `claim_id`, `cond_id`, proposal IDs) come from read-only SQL in the sandbox, keyed by what the messages show: a URL with its observation time, a quote, a statement, a public time.
- **Implicit records take no ID:** the current event (in every tool scoped to it), the incoming source (`record_event`, `add_observation`, `get_item`) and the current proposal (`record_review`). Pass an ID only for a different record: `get_item` or `get_event` of another record, the `merge_into` target, the `add_observation` document of a callback without an incoming source (MILESTONE, CONDITION_FIRED, CALL_EXPIRED, USER_QUESTION), claim evidence from another document, assessment and beneficiary evidence (claims of the current event), `supersedes_claim_ids`, the watch to DISARM, `propose_pattern` evidence.
- A claim quoted from the incoming source needs no `evidence`: Tank cites the incoming source.

```python
import os
import psycopg

def q(sql, **params):
    with psycopg.connect(os.environ["TANK_DB_URL"]) as connection:
        return connection.execute(sql, params).fetchall()
```

## Source document by URL

```python
q("""SELECT item_id, source_id, document->>'evidence_kind' AS kind, observed_at, title
     FROM ingest.source_items WHERE url = %(url)s ORDER BY observed_at DESC""", url="https://…")
```

A page read with `fetch_page` is its own document (`source_id = 'WEB'`, `kind = 'FETCHED_PAGE'`; a `web_search` result is `SEARCH_RESULT`), even when a collected item has the same URL: cite the document whose text holds the quote. Several captures of one URL differ by `observed_at`; take the one with the observation time you were shown (`AND observed_at = %(observed_at)s::timestamptz`). A document without a URL: `WHERE strpos(document->>'text', %(excerpt)s) > 0`.

## The current event

```python
q("""SELECT e.event_id, e.statement, e.public_at, e.lifecycle
     FROM evt.events e
     JOIN evt.event_observations o ON o.event_id = e.event_id
     JOIN ingest.source_items i ON i.item_id = o.item_id
     WHERE i.url = %(url)s""", url="<URL of the incoming source>")
```

## Claims of an event

```python
q("""SELECT claim_id, claim_type, quote, subject, predicate, object, state_claim, verified
     FROM evt.claims WHERE document->>'event_id' = %(event_id)s ORDER BY claim_id""", event_id="EVT-…")
```

Claims of the event that contains a source URL, without its event_id first:

```python
q("""SELECT c.claim_id, c.quote, c.subject, c.predicate, c.object, c.state_claim, c.verified
     FROM evt.claims c
     WHERE c.document->>'event_id' IN (
       SELECT o.event_id FROM evt.event_observations o
       JOIN ingest.source_items i ON i.item_id = o.item_id WHERE i.url = %(url)s)
     ORDER BY c.claim_id""", url="https://…")
```

Narrow to one claim with `AND strpos(c.quote, %(quote)s) > 0`. Claim IDs sort in creation order. After `merge_into`, the claims belong to the surviving event.

## Events by statement, time or token

```python
q("""SELECT event_id, statement, public_at, known_to_us_at, lifecycle, event_state
     FROM evt.events
     WHERE lifecycle NOT IN ('CLOSED', 'MERGED', 'INVALIDATED')
       AND strpos(lower(statement), lower(%(words)s)) > 0
     ORDER BY known_to_us_at DESC LIMIT 20""", words="LIBRA")
```

By public time: `AND public_at BETWEEN %(start)s::timestamptz AND %(end)s::timestamptz`. By token: `AND document->'tokens_mentioned' @> jsonb_build_array(%(token_id)s::text)`. By entity: `AND document->'entities' @> jsonb_build_array(jsonb_build_object('entity_id', %(entity_id)s::text))`. A merged event names its survivor in `merged_into`.

## Watch conditions of an event

```python
q("""SELECT cond_id, token_id, status, document->>'role' AS role, document->>'human_text' AS text,
            document->'expr' AS expr, document->>'created_at' AS created_at
     FROM ana.watch_conditions WHERE event_id = %(event_id)s AND status = 'ARMED'
     ORDER BY document->>'created_at'""", event_id="EVT-…")
```

## Proposals by subject

```python
q("""SELECT prp_id, hook, concept_name, document->>'duplicate_of' AS duplicate_of,
            document->>'occurrences' AS occurrences, document->'evidence' AS evidence
     FROM scout.expansion_proposals
     WHERE human_action IS NULL AND strpos(lower(concept_name), lower(%(subject)s)) > 0
     ORDER BY prp_id""", subject="…")
```

## Registry keys

```python
q("SELECT token_id, symbol, document->>'project' FROM reg.tokens WHERE upper(symbol) = upper(%(ticker)s) OR lower(document->>'project') = lower(%(name)s)", ticker="UNI", name="Uniswap")
q("""SELECT DISTINCT e.entity_id, e.name FROM reg.entities e
     LEFT JOIN reg.entity_aliases a ON a.entity_id = e.entity_id
     WHERE lower(e.name) = lower(%(name)s) OR lower(a.alias) = lower(%(name)s)""", name="Uniswap Labs")
q("SELECT instrument_id, venue, market_type, venue_symbol FROM reg.instruments WHERE token_id = %(token_id)s ORDER BY venue, market_type", token_id="bitcoin:BTC")
q("SELECT source_id, name FROM reg.sources ORDER BY source_id")
```

A project or token outside the registry has no key: name it (`entities[].name`, `token_names`, `entity_name`, `token_name`); Tank refuses a name its registry resolves and asks for the key. To look one up, pass its name or ticker in `name` of `list_open_events` or `get_registry`; never compose a key for it.


# Executable sandbox access

Two read-only databases:
- **ClickHouse, the market data** (HLD §7.1): database `mkt` at `http://clickhouse:8123`, user `tank_agent`, the same password as in `TANK_DB_URL` (never print it). Tables `klines_1m`, `flow_bars_1m`, `oi_obs`, `funding_obs`, `liq_obs`, `tickers_24h`, `book_summaries` (columns below) and `trades` (every print: `instrument_id`, `trade_id`, `ts`, `side`, `price`, `qty_native`, `quote_notional`, `notional_usd`, …).
  - Always `SELECT … FROM mkt.<table> FINAL`: a row can have been rewritten (a repair, a late print) and FINAL returns its last version.
  - Always filter on the time column and `instrument_id`: only the partitions needed are read.
  - Numbers are stored as their exact decimal text: compute with `toDecimal256(c, 18)` or `toFloat64(c)`. JSON columns are text: `JSONExtractString(last_trade, 'trade_id')`.
  - Queries run read-only, at most 120 s and 1.5 GB each.
- **PostgreSQL, everything else** (events, items, cards, decisions, jobs, sessions): `TANK_DB_URL` as below.

```python
import os, json, urllib.parse, httpx
password = urllib.parse.urlsplit(os.environ["TANK_DB_URL"]).password
def ch(query, **params):
    r = httpx.post("http://clickhouse:8123/", params={"query": query + " FORMAT JSONEachRow", **{"param_" + k: v for k, v in params.items()}},
                   headers={"X-ClickHouse-User": "tank_agent", "X-ClickHouse-Key": urllib.parse.unquote(password)}, timeout=130)
    r.raise_for_status()
    return [json.loads(line) for line in r.text.splitlines()]
bars = ch("SELECT minute_start, buy_qty, sell_qty FROM mkt.flow_bars_1m FINAL WHERE instrument_id = {i:String} AND minute_start >= now() - INTERVAL 1 HOUR ORDER BY minute_start", i="binance:spot:BTCUSDT")
```

Connect only through the injected read-only `TANK_DB_URL`; never print that URL. PostgreSQL access is read-only as `tank_agent`. Its `search_path` covers every Tank schema (`reg, ingest, evt, mkt, ana, out, ops, scout, qa, public`), so `FROM claims` reads `evt.claims`; schema-qualified names work as well. The following examples perform reads; results from own scripts inform analysis and are not card numbers. Facts still require quotes stored through Tank tools.

```python
import os
import psycopg
import pandas as pd
with psycopg.connect(os.environ["TANK_DB_URL"]) as connection:
    recent = pd.read_sql_query("SELECT item_id,source_id,observed_at FROM ingest.source_items ORDER BY observed_at DESC LIMIT 20", connection)
```

Tank's filter and routing classifier is Microsoft-Decision-1 version 1 through Microsoft Foundry. The service uses `/providers/microsoft/v1/systemone`, the configured deployment name `decision-1`, and typed `state`/`questions` with `noul` probabilities. The Foundry credential belongs to the service and is not exposed to agent sandboxes. Read the recorded relevance, routing scores and `ops.llm_usage` provenance instead of calling a substitute classifier. Analyst/scout models use the Codex subscription gateway. There is no automatic model/provider fallback; replay only reuses an exact saved request. Historical Jev rows remain historical evidence, and the current 0.5 cutoffs are uncalibrated for Decision-1.

DuckDuckGo search needs **no API key** and costs zero. Use Tank's custom `web_search(query)` for facts: it stores titles, URLs and snippets as durable evidence. It uses pinned `ddgs==9.16.0`, exact DuckDuckGo backend only, 10 results, declared source User-Agent, at least 2 seconds between requests, and retry delays 5/15/45 seconds for the specified rate-limit/block/timeout failures. Its audited transport permits one `POST https://html.duckduckgo.com/html/` per attempt, does not follow redirects and does not load ambient proxies/credentials. It prevents DDGS metasearch fallback; a generic `DDGS().text(backend="duckduckgo")` is not a safe substitute because the library can substitute engines internally. Empty/failure results return explicit errors, never a paid or alternate-engine search. Search has no credential secret.

Etherscan reads use `GET https://api.etherscan.io/v2/api` with `chainid`, `module=account`, `action=tokenbalance` or `tokentx`, token contract/wallet and `apikey=os.environ["ETHERSCAN_API_KEY"]`. Use the verified token contract and chain ID; no ticker guessing. Do not print a request URL containing its API key. DefiLlama uses key-free `https://api.llama.fi/summary/fees/{verified_slug}?dataType=dailyFees|dailyRevenue|dailyHoldersRevenue`, `https://api.llama.fi/protocols`, `https://api.llama.fi/v2/chains`, `https://defillama-datasets.llama.fi/emissions/{unlock_slug}`, `https://stablecoins.llama.fi/stablecoincharts/{chain}`, and `https://yields.llama.fi/lendBorrow`. Fee protocol slugs and unlock slugs are different namespaces. Do not sum parent rollups and children twice. Missing provider metrics remain unavailable, never zero. Lending utilization fetches only `/lendBorrow`; its UUIDs are resolved using versioned `registry.yaml` `lending_pool_catalog` entries (`pool_id`, `project`, `chain`, `underlying_tokens`) with retained source hash/capture time. Runtime never fetches `/pools` for identity enrichment, and unknown UUIDs stay explicitly unmapped rather than guessing chain/protocol.

The sandbox does not call exchange APIs or WebSockets. Exchange data is in the `mkt.*` tables below, written only by tank-capture; read it there.


# Market tables in ClickHouse

The seven series and every print are in ClickHouse database `mkt` (HLD §7.1); the column lists below are generated from the producer contract (`MARKET_COLUMNS`). Each series is a `ReplacingMergeTree` on its identity: read it with `FINAL` so a rewritten row (a repair, a late print) reads as its last version, and filter on `instrument_id` and the time column so only the needed daily partitions are read. Decimal values are exact decimal text: compute with `toDecimal256(x, 18)` or `toFloat64(x)`, and never turn an unavailable USD value into zero. JSON columns are text: `JSONExtractString(last_trade, 'trade_id')`.

Every print is in `mkt.trades` (30 days; days of MAJOR+ events kept): per-trade questions (sizes, bursts, who traded first) read it directly, minute aggregates come from `mkt.flow_bars_1m`.

```sql
SELECT minute_start, minute_start_prec, buy_qty, sell_qty, sum_sq_qty,
       first_trade_id, last_trade_id, complete, gap_reason
FROM mkt.flow_bars_1m FINAL
WHERE instrument_id = {i:String} AND minute_start >= {start:DateTime64(6, 'UTC')} AND minute_start < {end:DateTime64(6, 'UTC')}
ORDER BY minute_start;

SELECT instrument_id, kind, as_of, as_of_prec, rate, rate_8h_eq,
       funding_time, funding_time_prec, source_observations, null_reasons
FROM mkt.funding_obs FINAL
WHERE instrument_id = {i:String} AND as_of <= {cutoff:DateTime64(6, 'UTC')}
ORDER BY as_of DESC LIMIT 20;

SELECT ts, side, price, qty_native, notional_usd
FROM mkt.trades FINAL
WHERE instrument_id = {i:String} AND ts >= {start:DateTime64(6, 'UTC')} AND ts < {end:DateTime64(6, 'UTC')}
ORDER BY ts, trade_id;
```

Ticker natural identity is instrument plus timestamp; funding identity is instrument plus kind plus as-of. Preserve component clocks and source observations when combining a mark/index/ticker point. Named structures such as `first_trade`, `boundary_segments`, `reference_points`, `source_observations`, `integrity`, `capture_proof` and `null_reasons` are evidence/provenance, not arbitrary row copies. Point/receipt/recording precision remains explicit; legacy unknown is null with its reason.

## mkt.klines_1m

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `c` | `Nullable(String)` |
| `code_version` | `Nullable(String)` |
| `complete` | `Nullable(Bool)` |
| `config_version` | `Nullable(String)` |
| `h` | `Nullable(String)` |
| `instrument_id` | `String` |
| `interval_s` | `Nullable(Int64)` |
| `l` | `Nullable(String)` |
| `market_type` | `Nullable(String)` |
| `n_trades` | `Nullable(Int64)` |
| `o` | `Nullable(String)` |
| `open_time` | `DateTime64(6, 'UTC')` |
| `open_time_prec` | `Nullable(String)` |
| `open_time_prec_reason` | `Nullable(String)` |
| `quote_asset` | `Nullable(String)` |
| `quote_usd_rate` | `Nullable(String)` |
| `quote_usd_rate_as_of` | `Nullable(DateTime64(6, 'UTC'))` |
| `quote_usd_rate_as_of_prec` | `Nullable(String)` |
| `quote_usd_rate_as_of_prec_reason` | `Nullable(String)` |
| `quote_volume` | `Nullable(String)` |
| `quote_volume_method` | `Nullable(String)` |
| `quote_volume_usd` | `Nullable(String)` |
| `quote_volume_usd_reason` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `source_archive` | `Nullable(String)` |
| `taker_buy_native` | `Nullable(String)` |
| `taker_buy_quote` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `venue` | `Nullable(String)` |
| `volume_native` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.flow_bars_1m

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `backfill_verified` | `Nullable(Bool)` |
| `bar_version` | `Nullable(Int64)` |
| `buy_qty` | `Nullable(String)` |
| `buy_usd` | `Nullable(String)` |
| `c` | `Nullable(String)` |
| `capture_method` | `Nullable(String)` |
| `code_version` | `Nullable(String)` |
| `complete` | `Nullable(Bool)` |
| `config_version` | `Nullable(String)` |
| `first_trade_id` | `Nullable(String)` |
| `gap_reason` | `Nullable(String)` |
| `h` | `Nullable(String)` |
| `instrument_id` | `String` |
| `l` | `Nullable(String)` |
| `last_trade_id` | `Nullable(String)` |
| `market_type` | `Nullable(String)` |
| `minute_start` | `DateTime64(6, 'UTC')` |
| `minute_start_prec` | `Nullable(String)` |
| `minute_start_prec_reason` | `Nullable(String)` |
| `n_buy` | `Nullable(Int64)` |
| `n_sell` | `Nullable(Int64)` |
| `notional_reason` | `Nullable(String)` |
| `o` | `Nullable(String)` |
| `quote_asset` | `Nullable(String)` |
| `quote_volume` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `revised` | `Nullable(Bool)` |
| `sell_qty` | `Nullable(String)` |
| `sell_usd` | `Nullable(String)` |
| `source_archive` | `Nullable(String)` |
| `sum_sq_qty` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `trade_aggregation` | `Nullable(String)` |
| `venue` | `Nullable(String)` |
| `volume_usd` | `Nullable(String)` |
| `vwap` | `Nullable(String)` |
| `zero_trade_minute` | `Nullable(Bool)` |
| `size_buckets` | `Nullable(String)` |
| `null_reasons` | `Nullable(String)` |
| `capture_proof` | `Nullable(String)` |
| `first_trade` | `Nullable(String)` |
| `last_trade` | `Nullable(String)` |
| `boundary_segments` | `Nullable(String)` |
| `reference_points` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.book_summaries

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `best_ask` | `Nullable(String)` |
| `best_bid` | `Nullable(String)` |
| `code_version` | `Nullable(String)` |
| `config_version` | `Nullable(String)` |
| `confirmation_observed_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `confirmation_observed_at_prec` | `Nullable(String)` |
| `confirmation_observed_at_prec_reason` | `Nullable(String)` |
| `exchange_ts` | `DateTime64(6, 'UTC')` |
| `exchange_ts_prec` | `Nullable(String)` |
| `exchange_ts_prec_reason` | `Nullable(String)` |
| `gap_reason` | `Nullable(String)` |
| `instrument_id` | `String` |
| `integrity_method` | `Nullable(String)` |
| `integrity_ok` | `Nullable(Bool)` |
| `levels_received` | `Nullable(Int64)` |
| `metric_key` | `Nullable(String)` |
| `mid` | `Nullable(String)` |
| `quote_asset` | `Nullable(String)` |
| `quote_usd_rate` | `Nullable(String)` |
| `raw_levels_range_pct` | `Nullable(String)` |
| `raw_levels_scope` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `second_path_confirmed` | `Nullable(Bool)` |
| `second_path_reason` | `Nullable(String)` |
| `spread_bps` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `depth` | `Nullable(String)` |
| `bids` | `Nullable(String)` |
| `asks` | `Nullable(String)` |
| `range_covered_pct` | `Nullable(String)` |
| `independent_rest` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.oi_obs

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `code_version` | `Nullable(String)` |
| `config_version` | `Nullable(String)` |
| `gap_reason` | `Nullable(String)` |
| `index_as_of` | `Nullable(DateTime64(6, 'UTC'))` |
| `index_as_of_prec` | `Nullable(String)` |
| `index_as_of_prec_reason` | `Nullable(String)` |
| `index_price` | `Nullable(String)` |
| `instrument_id` | `String` |
| `mark_as_of` | `Nullable(DateTime64(6, 'UTC'))` |
| `mark_as_of_prec` | `Nullable(String)` |
| `mark_as_of_prec_reason` | `Nullable(String)` |
| `mark_price` | `Nullable(String)` |
| `metric_key` | `String` |
| `oi_native` | `Nullable(String)` |
| `oi_usd` | `Nullable(String)` |
| `oi_usd_reason` | `Nullable(String)` |
| `raw_unit` | `Nullable(String)` |
| `raw_value` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `semantic_confidence` | `Nullable(String)` |
| `sidedness` | `Nullable(String)` |
| `source_archive` | `Nullable(String)` |
| `synchronized_eligible` | `Nullable(Bool)` |
| `timestamp_precision` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `ts` | `DateTime64(6, 'UTC')` |
| `ts_prec` | `Nullable(String)` |
| `ts_prec_reason` | `Nullable(String)` |
| `ts_bucket_size_s` | `Nullable(String)` |
| `ts_bucket_anchor` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.funding_obs

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `as_of` | `DateTime64(6, 'UTC')` |
| `as_of_prec` | `Nullable(String)` |
| `as_of_prec_reason` | `Nullable(String)` |
| `capture_source` | `Nullable(String)` |
| `code_version` | `Nullable(String)` |
| `config_version` | `Nullable(String)` |
| `funding_time` | `Nullable(DateTime64(6, 'UTC'))` |
| `funding_time_prec` | `Nullable(String)` |
| `funding_time_prec_reason` | `Nullable(String)` |
| `instrument_id` | `String` |
| `interval_h` | `Nullable(String)` |
| `kind` | `String` |
| `metric_key` | `Nullable(String)` |
| `primary_source` | `Nullable(String)` |
| `rate` | `Nullable(String)` |
| `rate_8h_eq` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `semantic_confidence` | `Nullable(String)` |
| `source_archive` | `Nullable(String)` |
| `source_path` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `source_observations` | `Nullable(String)` |
| `source_disagreement` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `null_reasons` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.liq_obs

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `id` | `Nullable(Int64)` |
| `code_version` | `Nullable(String)` |
| `config_version` | `Nullable(String)` |
| `coverage` | `Nullable(String)` |
| `instrument_id` | `Nullable(String)` |
| `metric_key` | `Nullable(String)` |
| `notional_usd` | `Nullable(String)` |
| `price` | `Nullable(String)` |
| `price_kind` | `Nullable(String)` |
| `qty_native` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `side` | `Nullable(String)` |
| `token_id` | `Nullable(String)` |
| `ts` | `DateTime64(6, 'UTC')` |
| `ts_prec` | `Nullable(String)` |
| `ts_prec_reason` | `Nullable(String)` |
| `usd_reason` | `Nullable(String)` |
| `null_reasons` | `Nullable(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.tickers_24h

| Column | ClickHouse type |
|---|---|
| `storage_key` | `String` |
| `capture_source` | `Nullable(String)` |
| `code_version` | `Nullable(String)` |
| `config_version` | `Nullable(String)` |
| `index_as_of` | `Nullable(DateTime64(6, 'UTC'))` |
| `index_as_of_prec` | `Nullable(String)` |
| `index_as_of_prec_reason` | `Nullable(String)` |
| `index_price` | `Nullable(String)` |
| `index_reason` | `Nullable(String)` |
| `instrument_id` | `String` |
| `kind` | `Nullable(String)` |
| `last` | `Nullable(String)` |
| `mark_as_of` | `Nullable(DateTime64(6, 'UTC'))` |
| `mark_as_of_prec` | `Nullable(String)` |
| `mark_as_of_prec_reason` | `Nullable(String)` |
| `mark_price` | `Nullable(String)` |
| `quote_asset` | `Nullable(String)` |
| `quote_volume` | `Nullable(String)` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `Nullable(String)` |
| `received_ts_prec_reason` | `Nullable(String)` |
| `recorded_at` | `Nullable(DateTime64(6, 'UTC'))` |
| `recorded_at_prec` | `Nullable(String)` |
| `recorded_at_prec_reason` | `Nullable(String)` |
| `source_provenance_complete` | `Nullable(Bool)` |
| `token_id` | `Nullable(String)` |
| `ts` | `DateTime64(6, 'UTC')` |
| `ts_prec` | `Nullable(String)` |
| `ts_prec_reason` | `Nullable(String)` |
| `usd_reason` | `Nullable(String)` |
| `volume_native` | `Nullable(String)` |
| `volume_usd` | `Nullable(String)` |
| `field_sources` | `Nullable(String)` |
| `unclocked_components` | `Nullable(String)` |
| `capture_sources` | `Array(String)` |
| `semantic_fingerprint` | `Nullable(String)` |
| `_version` | `UInt64` |

## mkt.trades

| Column | ClickHouse type |
|---|---|
| `instrument_id` | `String` |
| `trade_id` | `String` |
| `ts` | `DateTime64(6, 'UTC')` |
| `token_id` | `LowCardinality(String)` |
| `venue` | `LowCardinality(String)` |
| `market_type` | `LowCardinality(String)` |
| `venue_symbol` | `LowCardinality(String)` |
| `side` | `LowCardinality(String)` |
| `price` | `Decimal(38, 18)` |
| `qty_native` | `Decimal(38, 18)` |
| `quote_notional` | `Nullable(Decimal(38, 18))` |
| `notional_usd` | `Nullable(Decimal(38, 18))` |
| `quote_asset` | `LowCardinality(String)` |
| `usd_reason` | `LowCardinality(Nullable(String))` |
| `metric_key` | `LowCardinality(String)` |
| `trade_aggregation` | `LowCardinality(String)` |
| `raw_first_id` | `Nullable(String)` |
| `raw_last_id` | `Nullable(String)` |
| `sequence` | `Nullable(String)` |
| `sequence_semantics` | `LowCardinality(Nullable(String))` |
| `capture_source` | `LowCardinality(String)` |
| `ts_prec` | `LowCardinality(Nullable(String))` |
| `ts_prec_reason` | `LowCardinality(Nullable(String))` |
| `received_ts` | `Nullable(DateTime64(6, 'UTC'))` |
| `received_ts_prec` | `LowCardinality(Nullable(String))` |
| `received_ts_prec_reason` | `LowCardinality(Nullable(String))` |
| `code_version` | `LowCardinality(String)` |
| `config_version` | `LowCardinality(String)` |
| `_version` | `UInt64` |

# Market control and fundamentals tables in PostgreSQL

Read with `TANK_DB_URL`. These hold no exchange series or prints.

## mkt.supply_obs

| Column | PostgreSQL type |
|---|---|
| `available` | `boolean` |
| `chain` | `text` |
| `classification_required` | `boolean` |
| `code_version` | `text` |
| `config_version` | `text` |
| `fee_scope` | `text` |
| `instrument_id` | `text` |
| `kind` | `text` |
| `reason` | `text` |
| `received_ts` | `timestamptz` |
| `received_ts_prec` | `text` |
| `received_ts_prec_reason` | `text` |
| `recorded_at` | `timestamptz` |
| `recorded_at_prec` | `text` |
| `recorded_at_prec_reason` | `text` |
| `token_id` | `text` |
| `transfer_queries_succeeded` | `bigint` |
| `ts` | `timestamptz` |
| `ts_prec` | `text` |
| `ts_prec_reason` | `text` |
| `unlocks` | `jsonb` |
| `flows` | `jsonb` |
| `coverage_errors` | `jsonb` |
| `balances` | `jsonb` |
| `pools` | `jsonb` |
| `series` | `jsonb` |
| `metrics` | `jsonb` |
| `protocols` | `jsonb` |
| `identity_evidence` | `jsonb` |
| `inference_limits` | `text[]` |
| `unsupported_venues` | `text[]` |
| `semantic_fingerprint` | `text` |

## mkt.unlocks

| Column | PostgreSQL type |
|---|---|
| `code_version` | `text` |
| `config_version` | `text` |
| `date` | `timestamptz` |
| `date_prec` | `text` |
| `date_prec_reason` | `text` |
| `evidence_mode` | `text` |
| `instrument_id` | `text` |
| `kind` | `text` |
| `label` | `text` |
| `observed_at` | `timestamptz` |
| `observed_at_prec` | `text` |
| `observed_at_prec_reason` | `text` |
| `qty` | `numeric` |
| `received_ts` | `timestamptz` |
| `received_ts_prec` | `text` |
| `received_ts_prec_reason` | `text` |
| `recorded_at` | `timestamptz` |
| `recorded_at_prec` | `text` |
| `recorded_at_prec_reason` | `text` |
| `sale_inferred` | `boolean` |
| `schedule_as_of` | `timestamptz` |
| `schedule_as_of_prec` | `text` |
| `schedule_as_of_prec_reason` | `text` |
| `token_id` | `text` |
| `semantic_fingerprint` | `text` |

`evidence_mode` = `HISTORICAL_RECONSTRUCTION` marks a schedule captured after the fact (`observed_at` is the capture time) that was public by `schedule_as_of`; such a row is known from the end of its `schedule_as_of` bucket. An unlock is potential supply, never a sale.

## mkt.hot_tokens

| Column | PostgreSQL type |
|---|---|
| `code_version` | `text` |
| `config_version` | `text` |
| `event_id` | `text` |
| `expires_at` | `timestamptz` |
| `expires_at_prec` | `text` |
| `expires_at_prec_reason` | `text` |
| `instrument_id` | `text` |
| `priority` | `bigint` |
| `received_ts` | `timestamptz` |
| `received_ts_prec` | `text` |
| `received_ts_prec_reason` | `text` |
| `recorded_at` | `timestamptz` |
| `recorded_at_prec` | `text` |
| `recorded_at_prec_reason` | `text` |
| `since` | `timestamptz` |
| `since_prec` | `text` |
| `since_prec_reason` | `text` |
| `token_id` | `text` |
| `semantic_fingerprint` | `text` |

## mkt.capture_gaps

| Column | PostgreSQL type |
|---|---|
| `code_version` | `text` |
| `config_version` | `text` |
| `end` | `timestamptz` |
| `end_prec` | `text` |
| `end_prec_reason` | `text` |
| `gap_id` | `text` |
| `instrument_id` | `text` |
| `kind` | `text` |
| `observed_at` | `timestamptz` |
| `observed_at_prec` | `text` |
| `observed_at_prec_reason` | `text` |
| `reason` | `text` |
| `received_ts` | `timestamptz` |
| `received_ts_prec` | `text` |
| `received_ts_prec_reason` | `text` |
| `recorded_at` | `timestamptz` |
| `recorded_at_prec` | `text` |
| `recorded_at_prec_reason` | `text` |
| `start` | `timestamptz` |
| `start_prec` | `text` |
| `start_prec_reason` | `text` |
| `status` | `text` |
| `token_id` | `text` |
| `semantic_fingerprint` | `text` |

## mkt.backfill_runs

| Column | PostgreSQL type |
|---|---|
| `anchor` | `timestamptz` |
| `anchor_prec` | `text` |
| `anchor_prec_reason` | `text` |
| `backfill_id` | `text` |
| `code_version` | `text` |
| `config_version` | `text` |
| `event_id` | `text` |
| `finished_at` | `timestamptz` |
| `finished_at_prec` | `text` |
| `finished_at_prec_reason` | `text` |
| `instrument_id` | `text` |
| `reason` | `text` |
| `received_ts` | `timestamptz` |
| `received_ts_prec` | `text` |
| `received_ts_prec_reason` | `text` |
| `recorded_at` | `timestamptz` |
| `recorded_at_prec` | `text` |
| `recorded_at_prec_reason` | `text` |
| `started_at` | `timestamptz` |
| `started_at_prec` | `text` |
| `started_at_prec_reason` | `text` |
| `status` | `text` |
| `token_id` | `text` |
| `result` | `jsonb` |
| `semantic_fingerprint` | `text` |

# Current executable information-source manifest

This manifest is generated from current `sources.yaml` and takes precedence for endpoints that moved after the HLD verification snapshot (notably DTCC and Robinhood). Source identity is unchanged. X list ID and session are operator-private; only the URL template is shown. The DTCC public query credential is extracted at runtime from the official bootstrap script and is not written here.

| Source | Name | Poll seconds | Adapter | Request |
|---|---|---|---|---|
| S01 | SEC | 60 | rss | GET https://www.sec.gov/news/pressreleases.rss |
| S01 | SEC | 120 | rss | GET https://www.sec.gov/news/speeches-statements.rss |
| S02 | SEC EDGAR | 120 | atom | GET https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=S-1&owner=include&count=40&output=atom |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=crypto&order=newest&per_page=20 |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=%22digital%20asset%22&order=newest&per_page=20 |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=bitcoin&order=newest&per_page=20 |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=ether&order=newest&per_page=20 |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=stablecoin&order=newest&per_page=20 |
| S03 | Federal Register | 300 | json | GET https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=securities-and-exchange-commission&conditions[agencies][]=commodity-futures-trading-commission&conditions[agencies][]=federal-reserve-system&conditions[agencies][]=comptroller-of-the-currency&conditions[agencies][]=treasury-department&conditions[agencies][]=financial-crimes-enforcement-network&conditions[agencies][]=foreign-assets-control-office&conditions[agencies][]=federal-deposit-insurance-corporation&conditions[term]=tokenized&order=newest&per_page=20 |
| S04 | CFTC | 120 | rss | GET https://www.cftc.gov/RSS/RSSGP/rssgp.xml |
| S04 | CFTC | 300 | rss | GET https://www.cftc.gov/RSS/RSSENF/rssenf.xml |
| S05 | Federal Reserve | 120 | rss | GET https://www.federalreserve.gov/feeds/press_all.xml |
| S06 | OCC | 300 | rss | GET https://www.occ.gov/rss/occ_news.xml |
| S07 | US Treasury + OFAC | 300 | html | GET https://home.treasury.gov/news/press-releases |
| S07 | US Treasury + OFAC | 300 | rss | GET https://ofac.treasury.gov/rss.xml |
| S08 | DTCC | 300 | dtcc_search | POST https://depositorytrustclearingcorporationproductionbw3fbrqw.org.coveo.com/rest/search/v2 |
| S09 | Binance | 30 | json | GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=20 |
| S09 | Binance | 30 | json | GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=161&pageNo=1&pageSize=20 |
| S09 | Binance | 30 | json | GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=49&pageNo=1&pageSize=20 |
| S10 | OKX | 60 | json | GET https://www.okx.com/api/v5/support/announcements?annType=announcements-new-listings |
| S10 | OKX | 60 | json | GET https://www.okx.com/api/v5/support/announcements?annType=announcements-delistings |
| S10 | OKX | 60 | json | GET https://www.okx.com/api/v5/support/announcements?annType=announcements-trading-updates |
| S11 | Bybit | 60 | json | GET https://api.bybit.com/v5/announcements/index?locale=en-US&type=new_crypto&limit=20 |
| S11 | Bybit | 60 | json | GET https://api.bybit.com/v5/announcements/index?locale=en-US&type=delistings&limit=20 |
| S11 | Bybit | 60 | json | GET https://api.bybit.com/v5/announcements/index?locale=en-US&type=latest_bybit_news&limit=20 |
| S12 | Kraken | 120 | rss | GET https://blog.kraken.com/feed |
| S13 | Robinhood | 300 | html | GET https://robinhood.com/newsroom/ |
| S14 | Venue instrument lists | 60 | instruments | GET https://api.binance.com/api/v3/exchangeInfo |
| S14 | Venue instrument lists | 60 | instruments | GET https://fapi.binance.com/fapi/v1/exchangeInfo |
| S14 | Venue instrument lists | 60 | instruments | GET https://api.exchange.coinbase.com/products |
| S14 | Venue instrument lists | 120 | instruments | GET https://api.kraken.com/0/public/AssetPairs |
| S14 | Venue instrument lists | 60 | instruments | GET https://www.okx.com/api/v5/public/instruments?instType=SPOT |
| S14 | Venue instrument lists | 60 | instruments | GET https://www.okx.com/api/v5/public/instruments?instType=SWAP |
| S14 | Venue instrument lists | 60 | instruments | GET https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000 |
| S14 | Venue instrument lists | 60 | instruments | GET https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000 |
| S14 | Venue instrument lists | 60 | instruments | POST https://api.hyperliquid.xyz/info |
| S15 | Uniswap | 600 | html | GET https://blog.uniswap.org/ |
| S16 | Ondo Finance | 600 | html | GET https://ondo.finance/blog |
| S17 | LayerZero | 600 | html | GET https://layerzero.network/blog |
| S18 | BlackRock | 600 | html | GET https://www.blackrock.com/corporate/newsroom/media/press-releases |
| S19 | Franklin Templeton | 600 | json | POST https://www.franklintempleton.com/api/articleApi |
| S20 | Circle | 600 | html | GET https://www.circle.com/pressroom |
| S21 | Tether | 300 | rss | GET https://tether.io/feed/ |
| S22 | Paxos | 600 | html | GET https://www.paxos.com/newsroom |
| S23 | Governance forums | 300 | rss | GET https://gov.uniswap.org/latest.rss |
| S23 | Governance forums | 300 | rss | GET https://governance.aave.com/latest.rss |
| S23 | Governance forums | 300 | rss | GET https://gov.ethenafoundation.com/latest.rss |
| S23 | Governance forums | 300 | rss | GET https://forum.cow.fi/latest.rss |
| S23 | Governance forums | 300 | rss | GET https://forum.morpho.org/latest.rss |
| S23 | Governance forums | 600 | rss | GET https://forum.euler.finance/latest.rss |
| S23 | Governance forums | 600 | rss | GET https://gov.1inch.network/latest.rss |
| S23 | Governance forums | 600 | rss | GET https://gov.near.org/latest.rss |
| S24 | Snapshot | 120 | graphql | POST https://hub.snapshot.org/graphql |
| S25 | GitHub | 600 | atom | GET https://github.com/Uniswap/v4-core/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/aave-dao/aave-v3-origin/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/cowprotocol/services/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/hyperliquid-dex/hyperliquid-python-sdk/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/1inch/swap-vm/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/elliottech/lighter-python/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/ava-labs/avalanchego/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/near/nearcore/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/worldcoin/world-chain/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/PlasmaLaboratories/node-templates/releases.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/LayerZero-Labs/LayerZero-v2/commits/main.atom |
| S25 | GitHub | 600 | atom | GET https://github.com/morpho-org/morpho-blue/commits/main.atom |
| S26 | BNY | 600 | html | GET https://www.bny.com/corporate/global/en/about-us/newsroom.html |
| S27 | The Clearing House | 600 | html | GET https://www.theclearinghouse.org/About/Media/press-releases |
| S28 | Anchorage Digital | 600 | html | GET https://www.anchorage.com/insights |
| S29 | X | 120 | x | POST https://api.apify.com/v2/actors/rbf5CQKQgKNVl4SfO/run-sync?build=direct&memory=256&timeout=110 |
| S30 | BBC News | 120 | rss | GET https://feeds.bbci.co.uk/news/business/rss.xml |
| S30 | BBC News | 300 | rss | GET https://feeds.bbci.co.uk/news/technology/rss.xml |
| S31 | Bloomberg | 120 | rss | GET https://feeds.bloomberg.com/markets/news.rss |
| S32 | Reuters | 300 | rss | GET https://news.google.com/rss/search?q=site:reuters.com+crypto&hl=en-US&gl=US&ceid=US:en |
| S33 | CoinDesk | 120 | rss | GET https://www.coindesk.com/arc/outboundfeeds/rss/ |
| S34 | The Block | 120 | rss | GET https://www.theblock.co/rss.xml |

## Verified endpoint limit detail

Binance spot/futures weights depend on endpoint and limit; see config/api_limits.yaml and the documented used-weight headers. USD-M fundingRate/fundingInfo have a shared500requests/5minutes cap (Tank≤250), and openInterestHist has a separate1000requests/5minutes cap (Tank≤500). The native archive endpoints remain read-only and checksum verified.

Bybit official HTTP IP quota is600requests/5seconds, so tank-capture budgets at most300/5seconds. Hyperliquid official aggregate HTTP quota is1200weight/minute, Tank≤600: l2Book/allMids cost2; the ordinary info requests Tank uses cost20; userRole costs60. fundingHistory/recentTrades add weight per20returned items and candleSnapshot per60. Preserve these response-size weights. Etherscan free tier documents3calls/second and100000/day on selected chains; the service half-budget is1.5/second and50000/day; all sandbox sessions share the remaining half. An unsupported free-tier chain remains n/a.

OKX official endpoint limits are per2seconds; tank-capture reserves at most half:

| Endpoint | Documented requests/2s | Service budget/2s | Scope |
|---|---|---|---|
| /api/v5/market/books | 40 | 20 | IP |
| /api/v5/market/trades | 100 | 50 | IP |
| /api/v5/market/history-trades | 20 | 10 | IP |
| /api/v5/market/candles | 40 | 20 | IP |
| /api/v5/market/history-candles | 20 | 10 | IP |
| /api/v5/public/open-interest | 20 | 10 | IP_INSTRUMENT |
| /api/v5/public/funding-rate | 10 | 5 | IP_INSTRUMENT |
| /api/v5/public/funding-rate-history | 10 | 5 | IP_INSTRUMENT |
| /api/v5/public/instruments | 20 | 10 | IP_INSTRUMENT_TYPE |
| /api/v5/market/tickers | 20 | 10 | IP |
| /api/v5/market/ticker | 20 | 10 | IP |
| /api/v5/public/mark-price | 10 | 5 | IP_INSTRUMENT |
| /api/v5/market/mark-price-candles | 20 | 10 | IP |

Official sources: https://www.okx.com/docs-v5/en/ ; https://bybit-exchange.github.io/docs/v5/rate-limit ; https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits ; https://docs.etherscan.io/resources/rate-limits . Verified on17October2026; provider changes require adapter verification, not guessed fallback limits.

Hyperliquid recentTrades has no documented maximum response count. The adapter reserves its documented base, serializes calls and debits actual returned-size weight before releasing the provider slot. It does not claim a mathematically proven precharge bound for an undocumented response size, and does not invent a maximum. OKX index-tickers additionally documents20requests/2seconds perIP (service reserve≤10).
