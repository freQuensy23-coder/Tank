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

Prompt version `jev-v2`, so usage rows and cache keys never mix with jev-v1 scores. Validation on real pairs
waits for the OpenRouter top-up.
