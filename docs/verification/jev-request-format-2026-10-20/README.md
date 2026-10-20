# Jev request format, jev-v1 → jev-v2 (2026-10-20)

User decision 2026-10-20 (via the lead): "fix the dumb prompt", launch at the uncalibrated midpoint 0.5/0.5,
calibrate later on real news (todo.md). `before.json` and `after.json` are the exact request bodies for three
items, produced by `examples.py` from the code before and after the change (no network).

1. FILTER_ITEM, The Block news item.
2. ROUTE_ITEM, a Reuters repeat of a known event (Gemini/CFTC settlement). Truth: same event.
3. ROUTE_ITEM, an OKX notice against another OKX event. Truth: different event.

## What was wrong in jev-v1

TypeSafe's guidance for Jev 1.13 (docs.typesafe.ai/model-jaggedness/jev-1.13, /primitives, /api):

- It reads questions literally.
- Accuracy drops when the state carries unrelated content.
- You should name state fields in the question.
- Boundary cases belong in the criteria, aligned with the instructions.

jev-v1 broke each of these:

- In routing, the item's fields sat at the top level next to `event`, while the question said "the item" and
  "the event described". Nothing in the state was called "item".
- The title was repeated as the body: `"X\n\nX"`, or `"X - Reuters\n\nX Reuters"`. Empty fields were sent
  (`"quoted": null`), and `author` was sent even when it only repeated `source`.
- The event carried internal encodings: `{"entity_id": "UNRESOLVED:Gemini", "role": "SUBJECT"}`, token ids, and
  a microsecond `known_to_us_at`. TypeSafe advises against asking Jev to compare dates.
- Long bodies were sent whole, up to 11k characters for SEC speeches.
- The criteria said only "Same event" / "A different event". The main error class, different notices from the
  same exchange or repo, sat only inside the instructions.

## jev-v2

- `state` is `{"item": {...}}` for FILTER and `{"item": {...}, "event": {...}}` for ROUTE. Questions name
  `` `item` `` and `` `event` ``.
- The item has `source`, plus `author` only when it differs from source, `title`, and `text` (the body without
  the repeated headline, whitespace collapsed, cut at a sentence near 2000 characters). `quoted` is sent only
  when present.
- The event has the agent's `statement`, `entities` as registry display names, `tokens` as symbols, and `stage`
  in lower case.
- The questions keep 03 §3.1's conditions and project list. The boundary cases moved into the criteria:
  "even if it reports nothing new or important"; "even one about the same project, company, exchange or
  token".

Prompt version `jev-v2`, so usage rows and cache keys never mix with jev-v1 scores. The item body is kept up to
12000 characters (see below). Jev now runs through OpenCode (`jev-1.13-free` without a key).

## Validation on OpenCode jev-1.13-free (2026-10-20 17:30 UTC)

`validate_v2.py` scored the same pairs in both formats on the same model: 31 true repeats, 59 different events
(39 that jev-v1 on OpenRouter scored at 0.5 or more, 20 below) and 40 filter items. Output:
`validation-run1.log`, `pairs.json`, `provider-log.json`.

| | Routing separation P(same > different) | Routing at 0.5, same / different passed | Routing at 0.7, same / different passed | Filter at 0.5, crypto / non-crypto passed |
|---|---|---|---|---|
| v1 on OpenRouter (old cache) | 0.831 | 29/31 / 39/59 | 20/31 / 13/59 | 16/20 / 0/20 |
| v1 on OpenCode | 0.836 | 29/31 / 34/59 | 21/31 / 11/59 | 16/20 / 0/20 |
| v2 on OpenCode (body cut at 2000) | 0.812 | 26/31 / 32/59 | 24/31 / 18/59 | 18/20 / 0/20 |

- The filter is a little better with v2.
- Routing is within the noise of 90 pairs. The format is not what limits it: Jev's same-event
  discrimination is.
- The 2000-character cut lost one true pair. A LayerZero post went from 0.88 to 0.05 because the relevant
  passage sits past 2000 characters. The cut is now 12000 characters, below Jev's accurate-mode limit of
  about 32 KB.
- The re-run with the new cut stopped at the free-tier limit.

Free tier:
- The first 260 requests all returned 200, with p50 latency 0.26 s and max 1.8 s.
- After about 266 requests (about 242k input tokens), every request returns 429 `FreeUsageLimitError` with
  `retry-after: 23022`, about 6.4 h.
- Production needs about 1,900 Jev calls a day, so it needs an OpenCode key (paid `jev-1.13`).
