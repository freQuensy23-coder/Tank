# Preliminary cards and timed reviews

MATERIAL events may produce a PRELIMINARY card when the analyst identifies a concrete sourced lead with a potentially substantial consequence. LOW event class still stops. LOW event certainty is preserved and displayed; it does not prevent this preliminary type. The agent supplies the finding, possible effect, and explicit missing confirmation.

Ordinary PARTIAL, FULL and UPDATE rules remain separate. The first ordinary card still requires the normal eligibility and analysis, even after preliminary publication. All versions use the same out.alerts event chain and Telegram thread. Preliminary cards have no market/action blocks and do not satisfy ordinary first-card delivery timing. No new tables or tools are introduced. Migration 006 extends the existing out.alerts.valid_kind check to accept PRELIMINARY; it does not rewrite historical rows.

A preliminary card requires current, accepted, quoted evidence. Stale, invalidated, closed and merged events are refused. An unchanged preliminary is suppressed, except after terminal delivery failure. Changed preliminary versions share the existing update interval; a newer version supersedes an older queued preliminary. After ordinary publication, preliminary publication is refused.

set_watch_condition accepts either existing token/metric conditions or review_at plus review_question. A timed review has token_id null, is stored in ana.watch_conditions and uses the same expiry, cancellation and per-event count limits. Its deadline must be in the future and before expires_at. EVAL_CONDITIONS sends CONDITION_FIRED once, with the question and original session, without reading market data. Terminal events cancel pending reviews.

The analyst investigates the question, records new observations and reassesses the same event. Another review requires an explicit new deadline. Armed reviews and recently queued reviews prevent inactivity closure from discarding that work. Original discovery/publication timestamps do not move.

Validation covers preliminary-to-ordinary delivery in one thread, unchanged ordinary gates, stale/terminal rejection, superseded evidence, retry after terminal delivery failure, timer idempotency, cancellation/expiry, old-event lifecycle and session archival. A PostgreSQL integration test exercises the actual producers and delivery ledger with a mocked Telegram transport.

Existing analyst sessions receive updated tool schemas and the preliminary/timed-review instructions through model context version 3 on their next continuation. No deployment or live session mutation is performed by this change itself.
