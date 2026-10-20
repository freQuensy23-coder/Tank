# Market Truth wait-for-history / refresh: real PostgreSQL regressions (2026-10-20)

Four-file Market Truth/capture/decision/replay change (baseline 0d3e0cb), run from ws/tank-truth on openclaw-dp.

| Run | Database | Result |
|---|---|---|
| pg1 | disposable DBs on the Tank PG cluster (admin.env) | 97 passed, 3 skipped (need QA :55437), 1 failed: test_scheduler_late_capture (memory backend, 10 s bound, backfill takes 10.3 s; see note) |
| pg55437 / pg55437db | QA PG 127.0.0.1:55437 (tank-market-pgtest), own tank_* DBs | test_truth_condition_postgres, test_market_condition_snapshot real loader, test_market_typed_capture real producer: 3 passed |
| pg2 | Tank PG cluster | 8 passed; 5 failed with "out of shared memory / max_locks_per_transaction" (cluster shared by 9 sessions) |
| pg3 | own QA PG 127.0.0.1:55438 (max_locks_per_transaction=1024) | the same 5: 5 passed |

Key PG cases passed: test_wait_data_watch_refreshes_truth_before_decision_and_replays_lost_ack[postgres],
test_condition_firing_during_refresh_wait_is_deferred_until_its_own_refresh[postgres],
test_cancelled_preflight_reuses_job_before_any_decision_ledger[postgres],
test_truth_job_exact_spec_keys_priorities_and_cross_call_idempotency[postgres].

Note: test_scheduler_late_capture fails on timing only. MarketService backfill in the memory repository
(market_storage.py:236 linear identity scan) takes about 10.3 s against a 10 s bound on the loaded host. Reported to tank-market.
