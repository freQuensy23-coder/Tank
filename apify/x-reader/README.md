# X direct-fetch Actor, version 0.2

This source is prepared for the existing Actor rbf5CQKQgKNVl4SfO under the build tag `direct`.
It is not published until deployment and the cloud checks succeed.

The application calls POST /v2/actors/rbf5CQKQgKNVl4SfO/run-sync?build=direct&memory=256&timeout=110
with an Authorization Bearer Apify token and JSON containing handles, optional concurrency,
optional cursors (account to previous maximum post ID as string), and optional requestId.

The response is the OUTPUT JSON object. It contains all currently visible own posts, account
results, errors, gap reports, and nextCursors. The first call also returns all visible posts.
The application owns baseline seeding, post ID deduplication, scheduling, notification queues,
and retries. Commit nextCursors in the same transaction that persists the returned posts.
Failed accounts keep their previous cursor. A reported coverage gap requires application handling.
Do not advance cursors if the response was not committed. Calls return snapshots, not only new posts.

HTTP success means the collection executed; inspect health, ok, failedAccounts, and coverageGaps.
A complete source outage returns health=failed and error=PUBLIC_SOURCE_UNAVAILABLE in OUTPUT;
partial failures return available posts with health=degraded. Invalid input fails the Actor.

The adapter reads INPUT and writes OUTPUT once. It creates no named state, history journal,
datasets, callback requests, or request queue. Normal Apify execution still stores INPUT and OUTPUT.
The two-write cost expectation requires verification of actual cloud run usage after deployment.

Run tests on openclaw-dp from this directory:
../.venv-direct/bin/python -m pytest -n auto -m 'not integration'
../.venv-direct/bin/python -m pytest -n auto -m integration
../.venv-direct/bin/ruff check .
../.venv-direct/bin/ruff format --check .
../.venv-direct/bin/mypy main.py direct_fetch.py public_profile.py

Deployment helper in the parent directory:
python3 deploy_direct.py --prepare-only
APIFY_TOKEN supplied in environment: python3 deploy_direct.py --pause-legacy
The helper publishes version 0.2 with tag direct, verifies the synchronous response and storage
usage, and only then disables the existing two-minute schedule when --pause-legacy is supplied.
It does not change the Actor default latest build or the shared Tank application.
Existing named state and pending notifications are preserved for application-side migration.

Public X pages usually expose the latest five own posts. More than five between calls can
leave gaps. Protected posts and other authors' reposts are not collected. No personal X account,
cookies, paid proxies, browser, or external data API key is used.
