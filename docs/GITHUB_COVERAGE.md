# GitHub coverage audit

Checked 2026-10-28T23:39:04.019083+00:00. Baseline: `ff6512a4ac2107319effc8e7745ee898a47c6212`. Scope: the 22 token-linked PROTOCOL entities in `config/registry.yaml`. The original 12 S25 feed objects and indexes are unchanged; 379 default-branch commit feeds are appended (391 feeds total).

## Inventory and evidence

The machine-readable [audit](github_coverage.json) records canonical repository identities, exact default branches, primary publisher links, materiality decisions, explicit implementation limitations and each retained Atom digest/latest entry/time. Public Atom bytes are preserved unchanged in `tests/fixtures/github_coverage/feeds.tar.gz`; this is historical source evidence, not a second runtime config. The two previously covered commit feeds are included in that archive.

Paginated publisher inventories were triaged for protocol/contracts, token/fee/governance mechanisms and material products. SDKs, hosted-product clients, production configuration, draft specifications and security reports are identified as supplementary; they do not establish core implementation coverage. Generic developer utilities, standalone analysis tools, examples, unrelated forks and unsupported materiality claims are excluded. Canonical conformance vectors and maintained execution dependencies remain material. Quiet releases and fork status alone are not exclusions. Independent reviewers screened every proposed ADD rationale and examined suspicious primary sources; this is not a complete implementation audit of every repository.

| Project | Added commits feeds | Public-source boundary |
|---|---:|---|
| Bitcoin | 5 | Material public code/specifications; publication does not establish deployment. |
| Ethereum | 71 | Core clients/specifications and material products; independent clients are not Ethereum Foundation-owned. Two historical client/adoption roles remain unverified. |
| Uniswap | 45 | Material public code/specifications; publication does not establish deployment. |
| PancakeSwap | 14 | Material public code/specifications; publication does not establish deployment. |
| Aerodrome | 3 | Material public code/specifications; publication does not establish deployment. |
| Curve | 49 | Material public code/specifications; publication does not establish deployment. |
| 1inch | 15 | Material public code/specifications; publication does not establish deployment. |
| 0x | 4 | Material public code/specifications; publication does not establish deployment. |
| CoW Protocol | 28 | Core services/contracts and grant products; current CoW AMM/MEV Blocker source boundaries remain qualified. |
| Hyperliquid | 3 | SDK, bridge and node distribution; HyperCore engine/consensus source unavailable. |
| Lighter | 7 | Proof circuits, L1 contracts and clients; production sequencer/matcher source not established. |
| Aave | 42 | Material public code/specifications; publication does not establish deployment. |
| Morpho | 11 | Material public code/specifications; publication does not establish deployment. |
| Euler | 10 | Material public code/specifications; publication does not establish deployment. |
| Ondo | 7 | Material public code/specifications; publication does not establish deployment. |
| LayerZero | 16 | Material public code/specifications; publication does not establish deployment. |
| Quant | 2 | PayScript/SAEP specifications only; current Overledger/Fusion/QNT runtime implementation not established. |
| Avalanche | 8 | Material public code/specifications; publication does not establish deployment. |
| NEAR | 17 | Material public code/specifications; publication does not establish deployment. |
| Plasma | 1 | Node configuration only; current consensus/execution binary source not established. |
| Ethena | 3 | USDe, SuiUSDe and USDm clients only; maintained core minting/hedging implementation not established. |
| World | 18 | World ID/protocol/orb/public chain development; private production integrations and billing deployment not established. |

## Explicit boundaries

This finite snapshot does not continuously discover new organization repositories or monitor all activity, nondefault branches, open pull requests or complete diffs. The raw organization inventories stay outside the repository; exclusion counts summarize scope triage, not line-by-line code audits. Historical economic code is not evidence of current deployment. `ethereum/alexandria` and `ethereum/trinity-eth2` remain role/support gaps rather than pretending they are current client coverage.

Per-feed publisher metadata names the actual GitHub namespace with a verified protocol context. Client vendors, grants publishers and independent foundations remain distinct owners. This context participates in routing identity matching; it is absent from Decision-1 item text. Technical artifacts retain TECHNICAL_EVIDENCE, FILTER_ITEM admission and priority 2.

The retained World ID Atom includes billing commit `111f5675534dbe317cf91bc7707bc5897889e72c`, with its subject and coauthor trailers. It contains no WLD/capacity explanation. Adding the feed repairs source coverage, not demonstrated economic-event recognition, live billing or Telegram delivery. Diff enrichment is outside this change.

## Polling and initial admission load

At the unchanged 600-second cadence the configured inventory schedules 39.1 feed requests/minute, versus 1.2 before. The existing source-wide 15-second escalation would schedule 1564/minute, versus 48 before; actual completions depend on transport, Retry-After and existing shared budgets. No endpoint capacity is invented and no new rate limit or polling architecture is introduced.

The retained new-feed windows contain 6,749 entries (6,689 distinct commit IDs); 249 distinct IDs fall in the preceding 24 hours. These are bounded Atom windows, not a complete daily-volume estimate. Initial empty checkpoints can admit historical entries to FILTER_ITEM; source/content deduplication reduces admitted rows. The prior 30-commit/day estimate does not cover this inventory.

The existing S25 unknown-limit policy and NOT_TESTED unattended-acceptance status remain unchanged. Snapshot HTTP200/XML validation does not qualify sustained publisher capacity or production database/LLM load. Those production-acceptance boundaries must remain visible; this PR makes no deployment or capacity claim.

## Permanent automatic qualification

The existing CI selects config/catalog contracts, exact archived Atom/XML/parser and routing-identity checks for every selected repository, ambiguous-name regressions, and World billing ingestion into an owned isolated PostgreSQL database. The latter retains the real public payload, raw hashes, source/filter jobs and idempotent repeated polling. CI receipts distinguish actual loopback HTTP acquisition from historical publisher availability. No production SQL, live model call, Telegram message, server setting or deployment is part of qualification.
