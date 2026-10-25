# Jev HTTP failure recovery

This change fixes loss of classification work after confirmed HTTP 429 responses.
It does not remove a provider quota or claim that a model outage has ended.

## Incident evidence and its limits

The Bloomberg item `ITM-01M5SD95BB97S4CWNQ0AD8WCGJ` was received on
2026-10-25 at 11:31:36 UTC. Its FILTER_ITEM job
`JOB-01M5SD95CVPYT7EVE3QSS3HFX3` failed twice, at 11:31:37 and 11:31:39 UTC,
then became FAILED with `attempts=2` and `max_attempts=2`.
Its original expiry was 17:31:36 UTC. The filter decision remained null.

The original handler stored only `HTTPStatusError`; the original response status
and body are unavailable. Two later diagnostic requests from the deployed
container with the same item, model and credentials returned HTTP 429 and
“Upstream request failed: Rate limit exceeded. Please try again later.”
The second response, at 11:45:47 UTC, included `Retry-After: 13`.
These probes demonstrate a reproducible rate-limit failure, not the unavailable
status codes of the original two requests.

## Behavior

- A confirmed Jev HTTP 429 returns FILTER_ITEM or ROUTE_ITEM to the durable queue.
  It refunds only that processing attempt. It does not undo earlier real failures.
- The next attempt is no earlier than Retry-After (delta seconds or HTTP date).
  Repeated refusals use increasing backoff and jitter; missing/invalid headers
  use the same backoff. The worker never sleeps while holding the item lock.
- Waiting retains the original six-hour expiry. A delay past the deadline ends
  in EXPIRED without another provider call. A restart retains the delay and evidence.
- Other HTTP errors retain the existing failure budget and now retain their
  actual HTTP status in last_error. There is no blanket retry of every error.
- Stored 429 diagnostics contain status, fixed provider/model/endpoint labels,
  the standard HTTP reason, a validated correlation ID and parsed retry delay.
  Raw response bodies, request headers and credentials are not copied into jobs.
- Parallel routing honors the longest Retry-After across all rate-limit responses.
  A concurrent real error still spends an attempt and retains its status; its retry
  also respects the outstanding rate-limit delay.
- Manual recovery and failure reconciliation use the same lock; reconciliation
  rereads the current state so a stale scan cannot overwrite recovery or completion.
- Successful scores remain cached. A retry after partially completed routing
  reuses those scores and queues only one agent message.

## Recovery of an existing failed job

After the change is deployed, an operator may explicitly select an unexpired
failed Jev HTTP job:

```sh
tank jobs retry-jev JOB-01M5SD95CVPYT7EVE3QSS3HFX3
```

This changes the existing job to QUEUED and resets its processing attempts.
It preserves the original failure in manual_retries, the original item and
idempotency key, and the original deadline. It refuses completed, expired,
unrelated or already-queued work. It sends no HTTP request or Telegram message;
the running service subsequently claims the job.

Legacy HTTPStatusError jobs are never automatically assumed to be 429s or
recovered in bulk. The command is an explicit operator decision about one job.
If the original deadline has passed, this command refuses recovery.

## Verification

`tests/test_jev_rate_limits.py` exercises the real classifier, pipeline and worker
with controlled provider responses and a virtual clock: 429 → 429 → 200,
not-before enforcement, rejection and acceptance, real error budgets, persistent
outage expiry, partial routing caching, safe diagnostics and targeted recovery.

`tests/test_jev_rate_limits_postgres.py` exercises actual HTTP responses from a
loopback server and actual PostgreSQL transactions. It closes and reopens the
repository after the first 429, checks that the delay survives, explicitly moves
the test job's schedule forward to avoid wall-clock sleeps, and verifies a single
stored score and at most one downstream routing job. Provider scores in these
tests are controlled fixtures, not live Jev results.

CI runs the complete unit and integration collections and builds all five images.
This MR does not deploy or mutate the failed production job.
