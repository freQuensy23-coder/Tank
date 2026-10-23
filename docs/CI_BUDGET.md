# Four-minute MR feedback budget

The first CI result is measured from the pull request's `created_at`, including event delivery, runner queueing, environment preparation, every required test/lint lane and all five image builds. A later commit gets a new 240-second feedback window from that workflow run's `created_at`; the age of an existing MR is not a reason to reject every later change.

The `CI 4 minute budget` job polls this run's jobs. It requires successful completion of all named lanes and all five image builds. Missing, skipped, cancelled, failed or late results cannot turn the gate green. It reserves five seconds for finishing and requests force-cancellation only of its own workflow when the budget expires. Individual MR jobs also have a four-minute ceiling. Publishing and deployment remain separate; this MR does not change production.

GitHub-hosted runners do not guarantee scheduling latency. If the budget job itself is queued past the deadline, it rejects and cancels the run immediately when it starts. It cannot execute while queued. GitHub may also finish runner cleanup after accepting cancellation. A physical guarantee that work disappears at exactly 240 seconds requires an independently scheduled controller or reserved runner capacity; this workflow does not claim that infrastructure exists.

## Removing the measured delays

In MR #22, run [39973960566](https://github.com/freQuensy23-coder/Tank/actions/runs/39973960566), `playwright install --with-deps chromium` took 758 seconds. Its log shows slow package transfers and retries from `azure.archive.ubuntu.com`. The 352 integration tests themselves took 132.77 seconds. The workflow had a 20-minute integration timeout and no wall-clock MR gate.

Main now reads S19 through the Franklin Templeton JSON endpoint and removes Playwright, Chromium installation and the browser test from Tank. This workflow preserves that removal: the Python integration jobs have no browser installation, browser smoke test or browser opt-in flag. The OpenMA sandbox browser remains as defined by main. Python is provisioned by setup-uv once instead of also running setup-python.

MR image builds read the existing GitHub Actions layer caches but do not export them. The main push workflow populates the shared caches. Uploading large cached layers is not part of MR feedback: in run 39996451323, OpenMA finished building before the deadline but its cache export was still uploading when the budget expired. All five Dockerfiles are still built and must pass. The OpenMA Dockerfile uses COPY --link for the sandbox OCI directory so BuildKit can attach that layer without downloading and unpacking the unchanged OpenMA dependency filesystem first. The destination is a new directory, with the same files and ownership; no runtime commands follow that copy.

Integration runs as three independent shards, each with its own PostgreSQL and ClickHouse services. A stable hash of the repository-relative test file selects its shard. Existing `-n auto --dist loadfile` behavior is preserved inside each shard, so tests that reset shared tables in one file remain together. Production 20-second timeout tests retain their real duration and all four parameter combinations.

Every shard records the entire integration collection before execution and checks its assigned report with zero skips. The final `Python integration complete` job requires all three shards, identical full manifests, and exactly one passing JUnit entry for every collected integration case across the union. A missing shard, omitted case, duplicate case, collection drift, failure, error or skip fails CI. No test is moved out of the MR gate.

## Validation

Regression tests cover the partition, missing/duplicate/failed/skipped cases, divergent manifests, the MR creation timestamp, later commits, missing CI lanes, late success and expired queue budgets. Full CI supplies the final runtime measurement; a fast cancellation is not evidence of a successful test run.

The real PostgreSQL lock tests use a one-second heartbeat deadline and a 100 ms acquisition polling interval; the 10 ms fake-connection clock remains limited to in-memory lock tests. The real tests retain the 2.5-second exclusion assertion and explicit backend termination/reacquisition checks, and release acquired locks on assertion failure. This removes a race-detector failure caused by applying a 10 ms simulated-network budget to an actual PostgreSQL connection.
