# Jev calibration

Run one command before choosing new cutoffs:

```sh
uv run tank calibrate frozen-packet.json --output-dir new-experiment
```

It calls only `POST https://openrouter.ai/api/v1/systemone`, with request model
`typesafe/jev-1.13` and the pinned resolved snapshot
`typesafe/jev-1.13-20260917`. Normal Settings resolution reads the OpenRouter key
from `.env` or its Docker secret. No service, native agent, collectors, delivery
or X session starts. Production configuration is never changed by this command.

The single JSON packet is independently labeled and frozen before scoring:

```json
{
  "dataset_id": "review-YYYY-MM-DD",
  "status": "FROZEN",
  "synthetic": false,
  "labeler": "reviewer identity",
  "labeled_at": "2026-10-20T00:00:00Z",
  "candidate_set_description": "Complete preceding captured event/session states",
  "export_provenance": "Location and review of the original source/runtime export",
  "complete_prefix_export": true,
  "items": [
    {
      "item_id": "ITEM-ID",
      "source_id": "S30",
      "label_group": "INDEPENDENT-EVENT-GROUP",
      "relevant": true,
      "cross_source_repeat": false,
      "expected_event_id": null,
      "source_item": {
        "item_id": "ITEM-ID",
        "source_id": "S30",
        "source": "Original source name",
        "author": "Original author",
        "title": "Original headline",
        "text": "Original collected text",
        "observed_at": "2026-10-18T12:00:00Z",
        "content_hash": "original content hash",
        "raw_sha256": "original raw artifact hash"
      },
      "prefix": {"events": [], "sessions": []}
    }
  ]
}
```

Supply at least 200 labeled news items, including at least 30 repeats/developments
and 10 cross-source repeats. Each prefix contains the complete actual preceding
event and session records at that item's original observation time. Event records
include `event_id` and `known_to_us_at`; sessions include `session_id`, `event_id`
and `last_item_at`. Empty lists are explicit. Future states are refused before any
paid call. Source metadata such as S25 `subtype` and S29 `author_role` stays in the
original source item. `expected_event_id` identifies a true antecedent even when
the runtime candidate prefilter misses it; a missing candidate remains a measured
miss.

The command uses the service's exact registry identity matching, three-day
candidate window, stable candidate order, state construction, questions and
classification adapter. Labels never enter a Jev question or event state. There
is no calibration-specific semantic rewriting of claims, stages or identities.

The report selects the highest filter cutoff retaining at least 95% of relevant
items, and the highest routing cutoff with both precision and recall at least
90%. If no routing threshold qualifies, the report says so and separately shows
the best measured F1 and its threshold. This does not apply that threshold or
claim 90/90. A proposal requires recorded review and separate D6 retesting.

The output contains immutable original inputs, code/config/registry generation,
request/response attempts without authorization headers, successful usage rows,
scored items, `calibration-report.json` and `thresholds.proposed.yaml`. Restarting
the same command reuses only successful scores for exact identical inputs and
code/config; changed inputs require a new directory. Failed attempts remain.
Synthetic test packets cannot produce production-eligible proposals.

The user-authorized deployed baseline remains `jev-v1`, filter 0.23 and routing
0.67. Its measured 1403-item result and independent D6 evidence remain in
[the selection record](verification/prompt-selection-2026-10-20/selection.json).
The user approved proceeding with that best measured baseline despite unmet
90/90; this command does not reopen or silently change that decision. Earlier
multi-file acquisition/reference/cache protocols and their evidence are historical
records, not additional supported calibration modes.
