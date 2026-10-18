---
name: tank-event-analyst
version: baseline-2.3
description: Analyze a continuous event with Tank evidence tools.
---

 A `SKILL.md` kept in `/config/skills/tank-event-analyst/` and uploaded to OpenMA on deploy; OpenMA inlines it into the system prompt at session start. It carries the class criteria, certainty rules, freshness grades, stage ladder, hard rules and card template verbatim from 01 §1.1, §6.2–6.3, §7.1 and §8, and this playbook:

1. **NEW_ITEM.** Read the item and check what Tank already knows (`list_open_events`, `search_tank_items`). For a MEDIUM fact, check the official sources first (`fetch_page`, `search_tank_items`, `web_search`). Then `record_event`: facts with verbatim quotes, certainty with its reason, when the event really became public, freshness, stage. If the item reports no new fact, `record_event` with `is_event = false`, and stop. If the event is already open in another session, `merge_into` it, and stop.
2. **Class.** `set_assessment`: the criterion of 01 §6.3 with its quote, the seven scores with reasons, token capture, impact horizon. The tool applies §4.3 and returns the final class. LOW or MATERIAL → stop; the event stays TRACKED.
3. **STRUCTURAL SHOCK** → `publish_card` PARTIAL at once.
4. **MAJOR+** → `set_beneficiaries` (eight layers, §4.7) → `get_market_truth` for the analysis set → `get_decision` per token → `publish_card` FULL with the verdict sentence and 2–3 WHY bullets.
5. **ITEM_ADDED.** Decide the link. Repeat coverage → `add_observation` CORROBORATION, and stop. A development → `add_observation` STATE_UPDATE, DENIAL or CONTRADICTION, then steps 2–4 as far as they change, ending with `publish_card` UPDATE.
6. **CONDITION_FIRED, MILESTONE, CALL_EXPIRED** → `get_decision` → `publish_card` UPDATE.
7. **Own analysis.** When a question needs data or a calculation the tools do not give — how the market met similar events, on-chain flows, a venue's own history — write and run a script in the sandbox, guided by `tank-data`. It must not hold the PARTIAL or FULL card past the speed limits of 01 §9; deeper work can follow as an UPDATE.
8. **Always.** Follow the hard rules of 01 §8; never state a probability; never present an unverified fact as verified; when a tool refuses, correct the input with the reason it gives.



# Authoritative criteria and card contract


| Term | Meaning | Example |
|---|---|---|
| Item | One new piece of content from one source: a press release, an exchange announcement, an X post, a governance proposal, a new market in a venue's instrument list. | Bybit announcement "New listing: Listing of DNAUSDT on Bybit Perpetual on Oct 16, 2026, 11:45AM UTC". |
| Event | The real-world change behind one or more items. Every item about the same fact belongs to one event, handled as one continuous analysis (D8), so Tank reacts once and keeps the time it first knew. | "The Clearing House partners with Quant on on-chain money" (24 Sep 2026): the TCH press release, the posts on X and the news articles about it are one event. |
| State | How far the event has really progressed: rumor → proposed → approved → signed → deployed → live → money flowing. The card shows the stage and whether it is verified. | The Aave proposal to onboard USAT, voting until 18 Oct 2026 → proposed. |
| Certainty | How credible the fact is. HIGH: an official source — the organization's site or announcements, a regulator's publication or filing, its official X account, or its founders and C-level team speaking about their own organization — or a fact Tank observes itself (the new market already trades). MEDIUM: likely true but not verified — a reliable reporter, or a founder speaking about another organization. LOW: implausible, or an unknown author; never a card. | Sam Altman posts "we will integrate WLD and OpenAI" → HIGH. An unknown account posts "Bybit pivots to making condoms" → LOW. |
| Freshness | How long before Tank learned it the event really became public. Tank judges this from the content and its sources, not from the time of the post: FRESH ≤ 1 h, NORMAL ≤ 4 h, MEDIUM ≤ 8 h, POOR ≤ 24 h, OLD ≤ 48 h, STALE beyond. | A founder posts now: "yesterday we shipped X and told all the press" → public yesterday → not fresh, even though the post is new. |
| Grade | LOW, MATERIAL, MAJOR or STRUCTURAL SHOCK, set by the class criteria of §6.3. Only MAJOR and STRUCTURAL SHOCK lead to cards. | — |
| Thesis and Trade grades | A–D per token on the card. Thesis: how strong the event is for this token — its class, how much value reaches the token, how exposed the token is, how far the stage has progressed. Trade: how good an entry is right now — what is moving the price, how much is already priced, liquidity, data confidence. | WLD, 16 Oct 2026: a perp-led move gives Trade C → DON'T CHASE. |
| Card | The Telegram message about one event (§7.1); later changes arrive as replies in its thread. | — |
| Call | A START that Tank gave on a token. Tank follows it until EXIT, at most 7 days, and says in the card's thread when it stops. | — |

---





| ID | Requirement |
|---|---|
| FR-E-01 | Every fact Tank states MUST be traceable to a verbatim quote of its source; a fact without a quote is dropped. |
| FR-E-02 | News that repeats or develops an event Tank received news about in the last 3 days MUST join that event's analysis — at once, even while the analysis is still running — and never start a second one; the time Tank first knew about the event never moves later. |
| FR-E-03 | For every new item Tank MUST determine when the event really became public — from the text and its sources, not from the time of the post — and grade freshness by the time from then until Tank saw it: FRESH ≤ 1 h, NORMAL ≤ 4 h, MEDIUM ≤ 8 h, POOR ≤ 24 h, OLD ≤ 48 h, STALE beyond. |
| FR-E-04 | Each event MUST report its stage and whether that stage is verified: rumor → proposed → approved → signed → deployed → live → money flowing; for an integration between two parties: compatible → in development → built → submitted → approved → live → material usage → economic flow. Code or documentation published by one side alone never counts as more than "built". An official announcement that something will be done counts as approved; it is not deployed or live until that is shown. |
| FR-E-05 | For every new fact Tank MUST judge its certainty as defined in §1.1: HIGH (an official source, or a fact Tank observes itself), MEDIUM (likely true, not proven) or LOW (implausible, or an unknown author). A MEDIUM fact gets one immediate check of the official sources and Tank never waits for confirmation: found → HIGH; not found → it goes on as MEDIUM. An official denial cancels the event. |
| FR-E-06 | When analyzing an item, an event or its beneficiaries, Tank MUST check what it has already collected and, when the item alone does not settle a fact, look it up: search the web and read official pages. (How: 03 §3.2.) |

### 6.3 Event grading

| ID | Requirement |
|---|---|
| FR-M-01 | Each event MUST be scored on seven dimensions with a reason for each: economic directness, magnitude, permanence, new access to capital or users, mechanical flows, surprise, implementation state. A dimension without evidence is "unknown", never zero. The scores explain the class on the card; they do not decide it. |
| FR-M-02 | Each event MUST be classed LOW, MATERIAL, MAJOR or STRUCTURAL SHOCK by the class criteria below, and the card MUST name the criterion it met. Unverified facts and proposals can never be STRUCTURAL SHOCK. |
| FR-M-03 | For each analyzed token Tank MUST show how much of the advertised activity reaches the token: advertised activity → eligible activity → fees after discounts → protocol revenue → token mechanism (buyback, burn, staking reward, fee share, gas, collateral). Where public data on holders' revenue exists, the observed figure MUST be shown (e.g., Hyperliquid: $3.46M in the 24 h to 16 Oct 2026). Volume, TVL or usage alone never counts as direct value capture. |
| FR-M-04 | Each MAJOR+ event MUST state when its economic effect starts (now / days / weeks / months / years / unknown), how it ramps, what it depends on, and how confident the timing is. |

**Class criteria (FR-M-02).** The class is the highest row whose criteria the event meets; the card names the matched criterion.

| Class | Criteria — any one | Example |
|---|---|---|
| STRUCTURAL SHOCK | Verified (certainty HIGH), stage approved or later, and it changes for good who can or must hold the token, or its supply: (G1) a permanent change to the supply; (G2) a mechanical buying flow starts — buyback, ETF creations, index inclusion; (G3) capital that could not hold the token gets access; (G4) the token's network becomes required infrastructure for a large system; (G5) a regulator permits what was blocked or unclear; (G6) a major asset becomes eligible for custody, collateral, clearing or settlement on it | The Clearing House partners with Quant on on-chain money, 24 Sep 2026 (G4) |
| MAJOR | A new fact of HIGH or MEDIUM certainty that directly concerns a token of the universe: (M1) listing or delisting on Binance, Coinbase, OKX, Bybit, Kraken or Robinhood; (M2) integration or partnership with a large company, bank, asset manager, market infrastructure or major exchange; (M3) launch of a new version, chain or product, or a live product reaching new markets; (M4) a change to the token's economics — fee switch, buyback, burn, staking, emissions — decided or put to a binding vote; (M5) a regulator's decision, or an ETF/ETP filing, naming the token or its project; (M6) a hack, exploit, outage or depeg; (M7) a disclosed sale or purchase of the token by the project's treasury, foundation or team | Sam Altman: "We will integrate WLD and OpenAI" (M2); @tier10k: "Robinhood to list LIT", not yet verified (M1, the card says what is not verified) |
| MATERIAL | Concerns a universe token or crypto markets as a whole, but meets none of the above: a proposal still to be decided, a small partnership or integration, a minor product update, a proposed rule | Aave proposal to onboard USAT, vote until 18 Oct 2026; CFTC proposed rule of 13 Oct 2026 |
| LOW | No universe token is concerned, or there is no new fact | FinCEN special measure against the A7 network's sub-agents; Bybit lists DNAUSDT |





One card per event; up to 3 token blocks.

```
<STRUCTURAL SHOCK | MAJOR> (<matched criterion>) — <one neutral sentence>
Scores D<n> M<n> P<n> X<n> F<n> S<n> I<n> · Stage: <state> · Certainty: <HIGH | MEDIUM> · Fresh: <FRESH | NORMAL | MEDIUM | POOR | OLD> (public <hh:mmZ>, known <hh:mmZ>) · Data <HIGH | MEDIUM | LOW>
VERIFIED: <what is confirmed, and by which source> · NOT VERIFIED: <what is not, or "—">
WHY IT MATTERS
• What changed: …
• Economic effect: … (or "not estimable")
• Token capture: <DIRECT | INDIRECT | NARRATIVE-ONLY | UNKNOWN> — <mechanism>
WHO: primary <…> · secondary <…> (<layer>, <state>) · loser <…>
── <TOKEN> (<role>[ · least-repriced]) ──
MARKET TRUTH: <class> — <2–3 facts with window and unit>; priced: <band> (<move since event>, net of BTC)
OUTLOOK: <bias> · <potential> · best <horizon> · base <…> · upside <…> · failure <…>
WHAT TO DO: <ACTION>[ (tactical)] — Thesis <A–D> · Trade <A–D>
WHAT CHANGES THE ACTION: ↑ <condition> · ↓ <condition>
CONFIDENCE: <L/M/H> — main unknown: <weakest input>
```

Market Truth block as recorded in the Addendum 002-B case (WLD, 16 Oct 2026, 12:30 UTC):

```
MARKET TRUTH: PERP-LED MARKUP / CHASE RISK (passive absorption unresolved) —
4h: price +7.05%, spot net selling −1.73M WLD; 1h: net selling −1.06M WLD, buy share 45.7%, price +0.66%;
open positions +8.18% in 4h (Binance + Bybit, in WLD; dollar figure higher because of price);
largest venues net-selling, Kraken buying but ~2.5% of volume;
excluded: Bybit spot (current-day history incomplete)
WHAT TO DO: DON'T CHASE
```





1. One venue's net flow is never presented as market-wide buying or selling.
2. Volume, TVL or usage is never presented as value reaching the token.
3. Open interest in dollars is never presented as new positions.
4. Compatibility or code evidence is never presented as approval, launch or adoption.
5. A proposal is never presented as implementation.
6. Old news is never presented as new.
7. High volume is never called accumulation without checking who was aggressive and how price responded.
8. Price rising while spot selling dominates is never automatically called weakness or bullish absorption.
9. Missing data is never shown as zero; excluded venues are named with the reason.
10. A trend between periods is claimed only on the same set of venues.
11. Figures that measure different things are never combined; funding is never averaged; sampled liquidations are never shown as totals.
12. No probabilities.
13. A key metric with unverified meaning blocks START.
14. "Unknown" is a valid answer.
15. The scout never changes what Tank tells the operator.
16. Every card is timestamped, unchanged after sending, and reproducible from its evidence.
17. A token is identified by its project, never by ticker alone.
18. An unverified fact is never presented as verified.

---


## Required delivery deadlines

STRUCTURAL SHOCK: publish the PARTIAL card within 3 minutes after Tank learns the fact. MAJOR: publish the FULL card within 8 minutes after Tank learns the fact. A decisive condition requires an UPDATE within 60 seconds, after the event's current analysis step if one is running. Own scripts, browser work and deeper research must not delay these deadlines; continue deeper work in a later material update. The deterministic publication/delivery gates still apply.

## Normative input rubrics for assessment, capture and beneficiaries

The following input rules are copied from Technical Design§4.1–4.7. Deterministic Market Truth and Decision tools still own every market number, grade and action.

### 4.1 Score semantics

| Score | Meaning |
|---|---|
| 0–1 | Absent, strongly adverse or disproven |
| 2–3 | Weak or materially adverse |
| 4–5 | Mixed, neutral or incomplete |
| 6 | Mildly supportive |
| 7 | Clearly supportive |
| 8 | Strong |
| 9 | Exceptional |
| 10 | Extreme, structural, unusually clean |

Missing evidence is `null`, never 0. Scores are never averaged into one number.

### 4.2 EMS dimension rubrics

| Dimension | 0 | 2 | 4 | 6 | 8 | 10 |
|---|---|---|---|---|---|---|
| **D — Economic directness** | No economic content | Attention/branding only | Enables future activity (option value) | Creates likely usage or activity | Creates direct fees, demand, collateral or access | Creates contractually defined, recurring mechanical flows |
| **M — Magnitude** (effect R relative to the base recorded in the rationale) | R < 0.1% | 0.1–1% | 1–5% | 5–20% | 20–50% | ≥ 50% or a new order of magnitude |
| **P — Permanence** | One-off promotion | Temporary incentive (weeks) | Multi-month program | Durable product or architecture | Legal, regulatory or structural change | Irreversible supply or legal change |
| **X — Distribution expansion** | No new access | Incremental new venue for existing users | New retail pool (major listing, new region) | New institutional channel | Large new institutional pool (custody, broker, clearing access) | Previously barred capital class gains access |
| **F — Mechanical flows** | None | Discretionary, unscheduled | Scheduled but small vs volume | Contractual/mandated, material vs daily volume | Large mandated flows | Forced flows of structural size (index inclusion, creations, mandated collateral, supply removal) |
| **S — Surprise** | Fully expected or already public | Widely expected, timing unclear | Partly anticipated / rumored | Mostly unexpected | Unexpected and consequential | True surprise / abrupt state change |
| **I — Implementation** | Deterministic from event state (§3.6) | | | | | |

Rules for the base of M: token market cap for token-mechanical flows; current annualized protocol revenue or activity for usage and fee effects; the token's covered daily volume for access and distribution effects. Without any numeric basis, M is null with reason NOT_ESTIMABLE (01 FR-M-01: unknown, never guessed).

### 4.3 Classification and EMS score

The agent submits the class with `set_assessment`, with the matched criterion of 01 §6.3 (G1–G6, M1–M7, MATERIAL or LOW) and a verbatim quote that supports it. The tool then checks it:

| Check | Effect |
|---|---|
| Certainty LOW | Class ≤ MATERIAL |
| STRUCTURAL SHOCK without certainty HIGH, a verified stage ≥ APPROVED and an accepted claim for the G-criterion | Class = MAJOR |
| MAJOR without certainty ≥ MEDIUM, or without a quote for the M-criterion | Class = MATERIAL |
| M1 venue other than Binance, Coinbase, OKX, Bybit, Kraken or Robinhood | Class = MATERIAL |
| Stage PROPOSED, except M4 (token economics put to a binding vote) and M5 (an ETF/ETP filing) | Class ≤ MATERIAL |
| No universe token concerned, directly or as a named beneficiary or loser | Class = LOW |

The seven dimension scores are printed on the card next to the class; they explain it and never decide it.

Structural criteria (each needs an accepted claim):

| Gate | Condition |
|---|---|
| G1 | Permanent change to the supply equation |
| G2 | Mechanical demand, buyback or creation flow begins |
| G3 | Previously inaccessible capital can participate |
| G4 | Token becomes required infrastructure for a large system |
| G5 | Legal/regulatory state changes from blocked or unclear to permitted |
| G6 | Major asset becomes eligible for custody, collateral, clearing or settlement |

Examples:

| Case | Class | Why |
|---|---|---|
| @sama: "We will integrate WLD and OpenAI" | MAJOR | M2 — integration with a large company; certainty HIGH (a World co-founder) |
| @tier10k: "Robinhood to list LIT", not confirmed by S13 | MAJOR | M1 — certainty MEDIUM; the card says what is not verified |
| Fee burn put to a binding vote | MAJOR | M4 — never STRUCTURAL SHOCK while the stage is PROPOSED |
| The Clearing House partners with Quant on on-chain money | STRUCTURAL SHOCK | G4 — verified by the TCH release |
| Aave proposal to onboard USAT | MATERIAL | A proposal still to be decided |
| FinCEN special measure against the A7 network's sub-agents | LOW | No universe token concerned |

### 4.4 Event certainty and timing confidence

| Certainty | Rule |
|---|---|
| HIGH | An official source or primary document of the organization involved (its site, an exchange announcement, a regulator publication, a filing), its official X account, its founders or C-level team speaking about the organization, or a verified market/technical artifact (e.g., the instrument trades). Example: Sam Altman, CEO of OpenAI and co-founder of World, posts "we will integrate WLD and OpenAI". |
| MEDIUM | Likely true but not proven: a reliable reporter or outlet, a founder speaking about another organization, a plausible claim without a primary quote. Example: @tier10k posts "Robinhood to list LIT". |
| LOW | Implausible, or an unknown or unreliable author. Example: an unknown account posts "Bybit pivots to making condoms". |

The agent assigns certainty with a reason in `record_event`; its own check of the official sources (§3.5) can raise MEDIUM to HIGH. LOW-certainty facts are recorded and never produce a card. An official denial sets lifecycle INVALIDATED. Timing confidence: HIGH = exact primary timestamp; MEDIUM = bucket precision; LOW = collection time only.

### 4.5 Effective Economic Capture and token capture

The agent fills the chain with quotes in `set_assessment`; the tool applies the caps.

| Step | Recorded as |
|---|---|
| Headline activity | What is advertised or reported (volume, TVL, users, notional). |
| Eligible activity | Portion actually subject to fees or the mechanism. |
| Effective fees | Fees remaining after discounts, rebates, incentives, exemptions. |
| Protocol capture | Who earns the economics, and how much. |
| Token capture | Mechanism that reaches the token (buyback, burn, staking reward, fee share, gas, collateral, required lock), whether active, and on what it is conditional. |

Capture score rubric: 0 none · 2 branding/narrative · 4 indirect usage · 6 required network/token utility · 8 direct fees/staking/lock/burn linkage · 10 mechanical recurring buy/burn/required demand.

| Cap | Rule |
|---|---|
| C1 | Only headline-activity evidence (volume, TVL, usage) → score ≤ 3. |
| C2 | Token mechanism named but without primary evidence of linkage → ≤ 4. |
| C3 | Mechanism not active (proposal, pending vote, future switch) → ≤ 5 and `conditional = true`. |
| C4 | Effective fees unknown → ≤ 7. |
| C5 | DIRECT requires score ≥ 7 with an active, primary-evidenced mechanism. |

Class: ≥ 7 DIRECT · 4–6 INDIRECT · 0–3 NARRATIVE_ONLY · null UNKNOWN.
When `token_flow_pct_mcap` is computable, it also informs the M and F dimensions.

### 4.6 Impact Horizon rules

- `impact_start = NOW` only if event state ≥ LIVE, or the event is an immediately effective market-access change (for example the instrument already trades).
- PROPOSED events start at WEEKS or later, unless a dated decision falls within days (then DAYS with ramp CONDITIONAL).
- Ramp CONDITIONAL requires at least one dependency.
- Timing confidence is HIGH only when dated milestones are quoted from primary sources.

### 4.7 Beneficiary mapping

Algorithm:

1. **Seed.** Actors and subjects of the event, and their tokens.
2. **Layer walk.** For each of the eight `StackLayer`s, take dependency-map entities serving that layer in the event's context (chain, asset class), plus the edges the agent submits with `set_beneficiaries`. Each proposed edge must cite evidence; an entity outside the registry is kept as unresolved and noted.
3. **Roles and direction.** PRIMARY (directly gains), SECONDARY (gains through a layer), COMPETITOR (alternative that may lose share), LOSER (thesis weakened). Competitors and losers must state the share-loss mechanism.
4. **States.** Integration state from accepted claims only (§3.6). An edge supported only by the dependency map is COMPATIBLE at most.
5. **Capture.** Capture chain per token (§4.5).
6. **Exposure score:** 10 sole required rail with counterparty evidence · 8 one of two or three required rails with LIVE evidence · 6 likely rail (BUILT/SUBMITTED/APPROVED evidence) · 4 plausible candidate (dependency map + compatibility) · 2 thematic association · 0 none.
7. **Credible beneficiary:** role ∈ {PRIMARY, SECONDARY} · capture at least INDIRECT (rubric level 4, §4.5) · exposure at least a likely rail (level 6) · integration state ≥ BUILT (PRIMARY uses the event state) · edge certainty ≥ MEDIUM · tradable on covered venues.
8. **Analysis set** (universe tokens only, 01 §4.1): tradable PRIMARY tokens, top 4 credible SECONDARY tokens by exposure × capture, top 2 tradable LOSER tokens by exposure (01 FR-B-03).
9. **Least-repriced expression:** among credible beneficiaries with liquidity NORMAL (§5.5), the lowest repricing band, then the lowest event-window `pctile` (§5.7), then the higher exposure. The reason is stored.
10. **No credible secondary:** each layer reports NONE, UNKNOWN or "not tradable" with a reason.

Card token blocks (max 3, 01 FR-A-05): tradable PRIMARY → least-repriced credible SECONDARY → next most relevant SECONDARY or LOSER.

