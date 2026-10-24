# X direct-fetch Actor

Published build: 0.2.2, tag `direct`, Actor rbf5CQKQgKNVl4SfO.
The Actor default build is direct; its permissions remain limited.
The old two-minute schedule has been disabled. The calling application schedules requests.

POST /v2/actors/rbf5CQKQgKNVl4SfO/run-sync?build=direct&memory=256&timeout=110
Authorization: Bearer an Apify API token. Body: handles, optional concurrency (1–8),
optional cursors (account to previous maximum post ID as a string), optional requestId.
Use an HTTP client timeout of 130 seconds. A successful synchronous response is HTTP 201.

The JSON response contains all visible own posts, account results, failedAccounts,
coverageGaps, health, ok, and nextCursors. It is not wrapped in data. Repeated requests
return snapshots again. The caller owns deduplication, baseline seeding, notification queues,
retries, and scheduling. Commit posts, queued notifications, and nextCursors atomically.
Do not advance cursors before persisting the response. Failed accounts keep the old cursor.

A successful HTTP response means collection executed; check health and error fields.
A complete source outage returns health=failed and error=PUBLIC_SOURCE_UNAVAILABLE;
partial failure returns good posts and health=degraded. Invalid input fails the Actor.

The adapter reads INPUT and writes one OUTPUT. It creates no named state or history,
Dataset records, callback requests, or Request queue records. Actual cloud usage on the full
86-account run 1rbkNSwdkR1gLHdcD confirmed two KVS writes and only INPUT/OUTPUT keys.
Cloud HTTP verification with an existing application token returned 422 posts from 86 accounts.

Tests: 31 unit tests and a live integration test passed; Ruff and mypy passed.
The source uses public X HTML without account cookies, browsers, paid proxies, or data API keys.
Public pages usually show five own posts; bursts can create reported gaps. Protected posts
and other authors' reposts are not collected. No strict latency guarantee is provided.

## In the Tank repository

This directory is the source of Apify Actor `fdfsfsdfasfa/tank-x-notifier` (ID `rbf5CQKQgKNVl4SfO`),
version 0.2, build 0.2.2 (`c99YRMlKXube5V0gV`), tag `direct`. The Python files are the deployed
version with Ruff formatting and import sorting applied; their syntax trees are unchanged.

### What it does

One run reads the public, logged-out profile page of every handle in `handles` and parses the visible
own posts. It compares them with the cursors the caller sends and writes a single `OUTPUT` record,
the JSON response described above. It keeps no state between runs and accepts no X credentials.

### How S29 calls it

Tank source S29 (`config/sources.yaml`, adapter `src/tank/sources/xsensor.py`) sends the registry
`x_accounts` handles and the stored cursors to

    POST https://api.apify.com/v2/actors/rbf5CQKQgKNVl4SfO/run-sync?build=direct&memory=256&timeout=110

with its Apify API key as a Bearer token. `build=direct` selects the build tagged `direct`, so a new
build reaches production only when it gets that tag.

### Tests

The unit tests run offline. They mock `fetch_profile` or parse the saved public page in
`tests/fixtures`. The repository lock does not include `apify` or `beautifulsoup4`, so these tests are
not part of the repository pytest lanes. Run them from this directory:

    uv run --no-project --python 3.12 --with-requirements requirements-dev.txt pytest -q

`tests/test_live.py` is marked `integration`. It reads the real public X pages of every
`x_accounts` handle in `config/registry.yaml`:

    uv run --no-project --python 3.12 --with-requirements requirements-dev.txt pytest -q -m integration

### Publishing a new build

1. Change the source here, run the tests, and merge to `main`.
2. Upload the files to Actor version 0.2. Either run `apify push` from this directory, or call
   `PUT /v2/actors/rbf5CQKQgKNVl4SfO/versions/0.2` with `sourceType: SOURCE_FILES` and `sourceFiles`
   for `main.py`, `direct_fetch.py`, `public_profile.py`, `Dockerfile`, `requirements.txt`,
   `README.md`, `.actor/actor.json` and `.actor/input_schema.json`.
3. Build it with `POST /v2/actors/rbf5CQKQgKNVl4SfO/builds?version=0.2&tag=direct&useCache=true`.
   When the build succeeds, the `direct` tag moves to it and S29 uses it on its next pass.
   To try a build before production, build it with a different tag and call `run-sync?build=<tag>`.
4. Wait until the build is `SUCCEEDED`, then check that one S29 pass reports `health=healthy`.
   To roll back, rebuild the previous commit with the tag `direct`.

Keep API tokens in the environment only. Do not commit run outputs, billing records or tokens.
