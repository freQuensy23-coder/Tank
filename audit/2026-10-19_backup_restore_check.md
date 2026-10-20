# Current backup and restore integration check

Executed 19 October 2026 against the current `Scheduler.maintenance` implementation.

Scope: four newly created, uniquely named fixture databases on the explicitly authorized local PostgreSQL 16 hostQA server, plus a pytest temporary directory containing synthetic data. No persistent Tank/controller data, subscription credentials, browser state, live raw captures, external API or model calls were used. All four fixture databases were dropped in the test's `finally` block.

The test called actual `maintenance(now, force=True, prune=False)`, using `/tmp/tank-pg16/bin/pg_dump`. The completed archive held exactly `tank.dump`, `oma.dump`, one content-addressed `.json.zst` bundle, `manifest.json`, and completion markers `COMPLETE`/`COMPLETE.json`. A synthetic raw-file sentinel outside `bundles/` remained in the fixture data directory and was absent from the archive.

Actual `pg_restore` restored both dumps into separate fresh fixture databases. Their seeded JSONB records matched, including the bundle hash reference. The archived bundle decompressed to the original canonical payload and its filename hash matched that payload.

With `file_digest` instrumented, two subsequent ordinary same-day maintenance calls performed **zero** archive digest reads. A fresh Scheduler instance performed **three** digest reads (both dumps and the bundle), fully verifying the existing archive. Explicit forced re-verification remains intentionally distinct from the ordinary warm-cache path.

Validation: `TANK_TEST_DATABASE_URL=postgresql://a.mametyev@127.0.0.1:55437/postgres uv run pytest tests/test_backup_hostqa.py -q -s` — **1 passed**. The opt-in test skips without that exact fixture controller URL. This is a focused current backup/restore proof, not a full-system restore claim; it does not supersede or relabel historical full-data backup evidence.
