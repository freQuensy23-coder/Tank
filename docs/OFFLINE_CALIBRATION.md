# Decision-1 calibration (`tank calibrate`)

The existing cutoffs remain the user-selected uncalibrated 0.5 midpoint. Calibrate Decision-1 independently on labeled data and record approved results in `config/thresholds.yaml` at a checkpoint (01 D6). Historical Jev evidence does not validate Decision-1. One command does the whole pass:

```sh
uv run tank calibrate packet.json --output-dir runs/<name>
```

It starts no service, session, collector or delivery. With the Foundry settings in [FOUNDRY_DECISION.md](FOUNDRY_DECISION.md), it calls `/providers/microsoft/v1/systemone` with the configured Decision-1 deployment through the service's own `Pipeline`: same item state, same
questions, same registry identities, same candidate builder. Production configuration is never changed.

## Packet

One frozen JSON file:

```json
{
  "dataset_id": "...", "status": "FROZEN", "labeler": "...", "labeled_at": "...",
  "routing_window_days": 3,
  "items": [
    {"item_id": "...", "source_id": "S30", "clock": "2026-10-16T11:34:54Z", "relevant": true,
     "event_id": "E1", "source_item": {"item_id": "...", "source_id": "S30", "source": "...",
     "author": "...", "title": "...", "text": "...", "quoted": null, "subtype": null, "author_role": null}}
  ],
  "events": [
    {"event_id": "E1", "statement": "...", "entities": [], "tokens_mentioned": [], "event_state": "PROPOSED"}
  ]
}
```

- `clock` is when the item became visible online (its publication time), not when an archive was collected.
  The 3-day routing window and every candidate set are evaluated at this clock.
- `relevant` is the reviewer's "about crypto" label (FR-D-06).
- `event_id` is the reviewer's event for every item that reaches ROUTE_ITEM (crypto, or an ALWAYS source).
  Items are replayed in clock order: the first item of an event opens it, each later one is a repeat or
  development that must join it. A later item more than 3 days after the event's last item must carry a new
  event (FR-E-02), otherwise the packet is refused. A routed item without `event_id` opens a session in
  which no event is extracted, so it never becomes a candidate (03 §3.1).
- `events` hold what the agent records for the event: the statement of the real-world fact, entities,
  tokens (registry token ids) and stage. `known_to_us_at` is the clock of the event's first item.

## Report

The output directory gets `calibration-report.json`, `scored-items.json`, `thresholds.proposed.yaml` and
`usage/`: one immutable Decision-1 usage row per exact request. A rerun reuses them and pays only for new requests.

- Filter cutoff: the highest value at which at least 95% of the items labeled relevant pass.
- Routing cutoff: the highest value at which routing precision and recall both reach 0.90. Per item the online
  rule applies: the candidate with the highest score at or above the cutoff gets the item. A wrong existing
  event counts as both a false positive and a false negative.
- V0-B checks: at least 200 items, 30 repeats and 10 cross-source repeats; at least 95% of crypto items reach
  the full analysis (ALWAYS sources, or filter score at or above the cutoff); a routing cutoff exists.
- `status` is PASS only when every check passes; only then does `thresholds.proposed.yaml` carry cutoffs.
  The full precision/recall curve is always reported.

First-known times, freshness grades and stages (the rest of V0-B) are the agent's outputs and are checked in
the session acceptance, not here.

The historical Jev packet and how it was rebuilt from the reviewed 1403-item set are in
`docs/verification/v0b-calibration-2026-10-20/`.
