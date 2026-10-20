# Read-only review of r82 recording latency

Scope: the thirteen completed `record_event` calls in the live partial timeline captured at 11:18:42Z. This is not the final run, a p95 certificate, or a proposal to change event decisions. No frozen source, prompt, schema, runtime, queue, data, or r82 resource was changed.

Exact safe phase metadata and derivations: [recording-phases.json](recording-phases.json). Its source timeline SHA is retained. Native SQL reads include only execution IDs/timestamps/event types and provider span IDs/timestamps/token counts; no prompts, credentials, native SDK or model calls.

| Phase, seconds | Minimum | Median | Maximum |
|---|---:|---:|---:|
| Source observation → recorded event returned | 120.575 | 213.590 | 319.146 |
| Initial native admission → first claim | 0.010 | 59.117 | 229.696 |
| Initial claim → first provider span | 13.371 | 40.063 | 46.659 |
| Actual paired provider request wall time before recording | 23.409 | 27.855 | 42.544 |
| Union of subsequent continuation queue intervals | 9.058 | 17.151 | 49.255 |
| External host-tool wall time before recording | 0.956 | 2.975 | 70.056 |

These rows overlap in places and are not an additive decomposition. Queue unions merge overlapping admitted→claimed requests; the earlier raw sums double-count waits when several tool results arrive together. Provider durations pair durable `span.model_request_start` / `end`, and are provider request wall time, not pure compute. Initial claim→first provider is explicitly pre-model overhead; the native startup owner is diagnosing it separately.

Example `EVT-01M5CGBTWQ56T8XKJZ467KHJBZ`: native initial queue 0.116s, initial claim→first provider 46.217s, four provider requests totaling 26.080s, external tools 2.565s, continuation queue union 39.365s. This event still took 120.575s. Calling its whole 114.6s claimed→record interval model computation would be wrong.

The cold 25-item arrival burst and four native execution slots explain why later items already wait up to 229.7s before their first execution. Removing one tool call cannot bring those initial waits below 90s. Keep the burst result separate from a fresh steady-arrival measurement, without moving the source clock or changing the 90s target.

## Current calls and prerequisites

- All thirteen sessions call `get_item`, although `SessionService.send_message` already sends the exact canonical item in `{"message": ..., "item": ...}`. The agent can read that object directly. The measured `get_item` tool wall time is only 0.02–2.43s; removing it is useful but not a claimed 30–60s saving. Most local reads were already emitted together.
- All thirteen do `list_open_events` and `search_tank_items`; those checks implement the required history comparison and should remain. Two sessions perform an additional history query; it is not automatically redundant because it can target an official source or disambiguate another fact.
- Every session fetches the source article plus performs one official check. This is not repeated official verification. S33 discovery RSS has empty body, so fetching its article is needed when a headline does not establish the claim or quote. Do not remove this evidence read merely to meet a clock.
- Five official `web_search` calls take about 65.8–69.3s; another takes 1.82s. Fixed DuckDuckGo retry behavior is required and stays unchanged. When a registry provides the correct official page, prefer one direct read of that page; when suitable already-collected official evidence exists, use it. Do not fabricate an official URL or treat unrelated cached absence as confirmation.
- The agent makes four to six actual provider requests before recording. Its last response generates roughly 447–827 tokens for `record_event`. Class scoring, market research and beneficiary expansion need not precede that call; only the minimal complete sourced event/uncertainty record belongs there.
- The native continuation coordinator currently orders eligible executions by `admitted_at_ms,id`. Queue observations do not prove an ordering bug. Giving every continuation priority over fresh input could worsen fresh-item latency while full analyses run; do not introduce that change without a separate measured scheduling design.

## Smallest isolated prompt iteration

Keep all normative claim, class, freshness, stage and official-check rules. Replace only the opening NEW_ITEM operational paragraph with a concise clarification along these lines:

> The incoming JSON `item` is the canonical collected item, already supplied with its text, source and timing provenance. Read it directly; call `get_item` only if the needed item is missing, incomplete/truncated, or a different history hit needs its full text. For NEW_ITEM, perform the required open-event/history comparison. Request independent reads together, including the source article when its supplied text cannot support the factual quote. Consult the registry only for an identity or official-source fact you still need. For a MEDIUM fact, perform the one immediate official-source check; prefer the correct registered official page or relevant already-collected official evidence when available. Keep the mandated search behavior when search is needed. After those initial checks, record, merge or dismiss promptly. An unavailable or inconclusive check leaves the claim unverified at its actual certainty; deeper research, scores and beneficiaries follow after recording. Do not wait for stronger confirmation or prepare the full assessment as a prerequisite. Preserve source clocks, unknowns and all supported distinctions between proposal, commitment and live availability.

This clarifies existing ownership and ordering; it does not make `record_event` mandatory for irrelevant items or impose a server-side tool-order validator. It does not promise 90s under a 65s mandatory blocked-search sequence plus cold startup.

## Citation correction in the same isolated iteration

The FINMA event `EVT-01M5CGEY2T4MX9YXA139T24MN3` contains three quotes whose stored evidence points to discovery item `ITM-01M5CG85DD42M60DQD77ZZDFGZ`, which has empty RSS text. Their actual carrying document is fetched item `web-4ec7dd01480acee20a364b363465f057`. Exact records are preserved in `normal-source-e2e-r82-2026-10-20/positive-interim-20261020T112230Z/` (`evt.claims.jsonl`, `ingest.source_items.jsonl`, `ops.tool_calls.jsonl`, manifest and original files). No current outcome was repaired.

This is partly a representation omission: both advertised claim schemas set `additionalProperties:false` but omit `evidence`; `EventToolService.validate_claims` already accepts it and otherwise defaults the quote to the root discovery item. A description-only instruction cannot reliably express a field the schema forbids.

Small compatible schema change to propose, without any host semantic validation: expose OPTIONAL `claims[].evidence` in both `record_event` and `add_observation`, mirrored in `tools.json`:

```json
{
  "type": "array",
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": ["item_id", "quote"],
    "properties": {"item_id": {"type": "string"}, "quote": {"type": "string"}}
  }
}
```

Add the explicit agent instruction:

> Cite the exact saved document that contains each quote. If the quote comes from `fetch_page` or `web_search`, use that result's `item_id` in the claim's `evidence`, even when its URL equals the discovery item's URL. Keep the root `record_event.item_id` as the discovery item so Tank's origin and first-known lineage stay correct. Use the discovery ID only for quotes actually present in that supplied item. Keep the top-level claim `quote` equal to the cited atomic quote. Tank does not infer, rewrite or validate the semantic citation on your behalf.

The fetched result already contains `item_id`, `text`, raw reference/hash and evidence kind. The host already persists supplied evidence, so no new semantic checker, record rewrite or framework is needed.

## Retest before adoption

After r82 finishes, register one isolated versioned skill/schema candidate. Reuse the same source evidence and unchanged model/effort/resources. Measure source→record, initial queue, claim→first provider, continuation queue union, model spans and external tool wall separately. Check evidence document IDs against actual saved quote carriers, original root lineage, certainty/stage/public clocks, behavior on unavailable official confirmation, and continuation as MEDIUM without repeated checks. Run both steady arrivals and the same unchanged 25-item cold burst; do not hide either distribution or claim the prompt alone fixes startup/capacity. Adopt only the reviewed iteration with its independent results.
