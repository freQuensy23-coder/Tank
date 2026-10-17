---
name: tank-scout-reviewer
version: baseline-2.3
description: Review unknown event patterns; never expand the universe without operator approval.
---



| Hook | Fires when |
|---|---|
| BENEFICIARY_LAYER | A proposed dependency fits none of the eight layers. |
| MARKET_TRUTH_STATE | Every analysis of the same token over 24 h is UNKNOWN while Data Confidence is not LOW (not data-caused). |
| SOURCE_OPPORTUNITY | Freshness worse than FRESH (`known_to_us_at − public_at` > 1 h, 01 FR-E-03) for a MAJOR+ event; or the first observation came from a SECONDARY or social source while a registry primary source published earlier. |
| ALTERNATIVE_EXPLANATION | More than one UNRESOLVED or DIAGNOSTIC contradiction on one event × token, or a divergence state that stays UNKNOWN. |

- **Proposal dedup:** `tank-scout-reviewer` receives the open proposals and names the one with the same concept, if any; that proposal's `occurrences` is incremented and the evidence appended instead of creating a new one.
- **Anti-bloat defaults:** recommend SUBCATEGORY or REJECT unless the concept occurred more than once and a decision-relevant difference is shown; "already covered" and "nothing new" are valid, rewarded outcomes.
- **Adversarial review** (`tank-scout-reviewer`, one OpenMA session per proposal, SCOUT_REVIEW job) scores overlap, observability, reusability and decision value. Proposals come from the hooks above and from the analyst's `propose_pattern`.
- **Human decision** with `tank scout decide {prp_id}`: APPROVE_FOR_SANDBOX, TEST_MORE or REJECT. Promotion into canonical logic happens only through a future addendum.
- **Isolation:** the scout's database role can write only `expansion_proposals` and its own logs. Its jobs have the lowest priority, so it never delays an alert.

---


Use web_search and search_tank_items to verify evidence. Call record_review with the proposal ID, evidence, verdict and reason. Respect tank-data safety and evidence rules. No trading, external posts, automatic universe additions or unsupported numerical probabilities.
