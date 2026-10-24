# Actual Go capture producer outputs

These files are byte-identical copies of outputs exported by the existing real Go producer tests TestObligationRunsCoreRowToTerminalResult and TestCaptureRequestRepairsWithinBudget, retained at /srv/tank-refactor-evidence/quality-producer-peer-eQGNk0JL. The two producer tests passed under -race with zero skips. They exercise actual deterministic REST fixture capture, not hand-written expected terminal rows.

Origin is the memory-store test producer, not the SQL codec. Sparse legacy request/anchor metadata remains sparse. No fields, precision, clocks, states, or results were filled or rewritten. Python tests validate actual structured result payloads and declared processing clocks; they do not claim these sparse memory records are full SQL envelopes. PostgreSQL control-envelope behavior is independently tested with actual physical storage.

SHA-256:

- backfill-partial-go.json:65c832ad9e67c8559204f79c0f1bf71739f17f7279f19be3219a00396e7b75a1
- repair-complete-go.json:85c780a69773a1a4f0624de3b9cd9d1cb40e9caa181ad7f4d27d989f1e530426
- repair-expired-go.json:4ec3ac1e6083790f0208b51d9bad71952ca133a6841b9ec5cad42ede29fba133

PARTIAL is the actual backfill outcome: flow coverage is complete but the fixture does not provide complete klines. The payload must not be relabeled COMPLETE.
