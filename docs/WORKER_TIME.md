# Where did the eight workers spend five minutes?

Use application INFO logs from **one Tank instance**, including its previous containers if it restarted. For example, for 12:00–12:05 UTC, export a wider interval from a local container (stderr included):

```sh
docker logs --since 2026-01-01T11:59:00Z --until 2026-01-01T12:06:00Z "$container" > worker.log 2>&1
uv run --frozen python -m tank.worker_time_report 2026-01-01T12:00:00Z 2026-01-01T12:05:00Z < worker.log > window.json
jq '{pool_us, by_worker: (.workers | map_values(.total_us))}' window.json
jq '.attempts[] | select(.worker == 2)' window.json
```

The end is exclusive; omit it for five minutes. Concatenate non-duplicated application lines, not Docker's raw JSON envelopes. Include records on both sides: samples normally flush every 30 seconds, and a later claim end can supply the exact outcome for a job crossing the window. Widen the export if an end is missing. Do not mix replicas or duplicate exports; overlapping worker evidence is rejected.

- `pool_us` divides eight workers' wall time among phases. Each worker's `total_us` sums to the window length, including `unknown`. These are not CPU or database server times.
- `queue_select` is time in `claim_job`, not idle. `empty_wait` is the configured pause after no job was returned, `reconcile` checks outstanding failures, and `claim_backoff` follows a selection error. `loop` is other between-claim overhead. Empty cycles batch their exact phase boundaries instead of emitting a line per poll.
- A claim moves through `prepare` (includes dependencies and expiry checks), optional `admission`, `handler`, and `settle` (result, deferral or failure handling). A span's `(worker, run, claim)` identifies its attempt, including the successful selection. A selection with no known winner has no claim number. A worker run is a fresh loop incarnation, not a durable host ID.
- `attempts` includes acquisitions or successful selections overlapping the window. Join `job_id` to `ops.jobs.job_id`; do not join by kind, nearest timestamp or database attempt count. Deferral can leave the next acquisition at the same attempt count. A null job ID means the returned ID was not a canonical Tank job ID. `select_ns` measures the successful poll; `elapsed_ns` measures acquisition through settlement. `eligible_wait_us` is lateness relative to this acquisition's `run_after`, **not** time spent selecting. Do not add it to worker totals.
- For a late job, inspect its selection duration, eligibility delay and all eight worker timelines. Long eligibility delays with short selections warrant checking competing work or queue eligibility; long selections warrant investigating the claim path. Long `handler` or `admission` spans point elsewhere. An expired job never returned by the queue has no claim to attribute.
- After abrupt death there is no invented end or outcome. `last_seen` is the last observed boundary; time not covered by retained records is `unknown`, never idle. A known end does not fill lost phase records. Use wider logs or report the missing coverage. UTC phase boundaries require an ordered wall clock; the exact completed claim duration uses the monotonic clock.
