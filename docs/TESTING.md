# Tests

Run commands in an isolated server-side development checkout with Python 3.13.13. Never use production credentials, production databases, or a personal browser profile.

## Commands

    uv sync --python 3.13.13 --frozen --extra dev
    uv run python scripts/ci/stage_retained_fixtures.py
    uv run pytest -q -n auto --dist loadfile

Run normal tests with the available CPUs and memory; do not add artificial CPU or memory quotas or affinity limits. pytest-xdist uses one worker per available CPU. Loadfile keeps each module on one worker, preserving module-scoped baseline reuse. In the same-source audit, all 1,671 units passed under both schedulers; loadfile took 55.395s including startup versus 72.039s for worksteal. Use `-n 0` for a serial diagnostic run.

The default Python selection includes tests without the integration marker. It has a standard pytest-timeout limit of 10 seconds per case, including setup and teardown. To check the p90 budget from standard pytest JUnit output, run:

    uv run pytest -q -n auto --dist loadfile --junitxml=.test-results/unit.xml
    uv run python scripts/check-unit-timings.py .test-results/unit.xml

Every unit case must take less than 10 seconds; p90 must be less than 1 second. The JUnit checker refuses empty, skipped, or failed runs. Suite wall time and individual case durations are different measurements.

Integration tests have a separate command and an explicit longer deadline:

    uv run pytest -q -n 0 -m integration --timeout=300

The explicit -m expression overrides the default unit selection. Normal pytest decorators and pytest.param marks determine selection; no external classification manifest is needed. PostgreSQL parameters carry their own marks, so the memory variant remains available to unit tests.

Resource markers allow narrower integration runs:

    uv run pytest -q -m 'integration and postgres' --timeout=300
    uv run pytest -q -m 'integration and browser' --timeout=300
    uv run pytest -q -m 'integration and not postgres and not browser and not node and not historical' --timeout=300

The postgres marker requires caller-owned disposable PostgreSQL 16 databases and explicit TANK_TEST_DATABASE_URL, TANK_E2E_DATABASE_URL, or TANK_SOURCE_TEST_DATABASE_URL as used by the selected fixtures. Each database name must start with tank_test. The shared lazy PostgresTemplate helper migrates a template once per worker and opens a fresh cloned database for each non-migration repository context. Migration regressions request a blank database explicitly. Pools close before clone cleanup; the descriptor holds no live pool across event loops. Keep the credential/database safety checks intact. CI additionally prepares one immutable, UUID-named schema seed through prepare_schema_template and passes --postgres-schema-template to collection and execution. The helper verifies the tank_test_ prefix, disabled connections, zero active sessions and the exact SQL/Python fingerprint before cloning. Every case still gets a fresh database; migrated=False remains blank, and tests never drop the caller-owned seed. Without the option, the lazy per-worker migration behavior is unchanged.

Before running migrations or preparing a schema seed, provision the actual tank_agent role on that caller-owned disposable cluster:

    CREATE ROLE tank_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
    ALTER ROLE tank_agent SET default_transaction_read_only = on;

These statements belong only to the isolated test cluster. The CI bootstrap in scripts/ci/prepare_postgres.py provisions this role before migrating its seed; the fixtures do not create or alter a pre-existing cluster role. A full manual integration run requires all three guarded PostgreSQL routes and the data/browser/Node/Go/Docker prerequisites below, with zero skips and zero failures. A missing prerequisite is not a completed qualification.

For portable parallel integration, the operator first provisions distinct disposable databases for every worker: for example, tank_test_integration_gw0 through tank_test_integration_gwN, and matching E2E/source administration databases if those routes are used. Use the same schema/template helper as the serial tests. No database may contain production data.

    export TANK_TEST_DATABASE_URL='dbname=tank_test_integration_{worker} host=/path/to/isolated-pg/socket user=postgres'
    export TANK_E2E_DATABASE_URL='dbname=tank_test_e2e_{worker} host=/path/to/isolated-pg/socket user=postgres'
    export TANK_SOURCE_TEST_DATABASE_URL='dbname=tank_test_source_{worker} host=/path/to/isolated-pg/socket user=postgres'
    PYTHONPATH=tests uv run pytest -p parallel_postgres -n auto --dist loadfile -m integration --timeout=300

The opt-in parallel_postgres plugin runs before conftest. The controller preserves the exact templates for worker inheritance; each gwN worker replaces exactly one literal {worker} in dbname only. Every configured route is validated before any environment update. Shared DSNs, non-test database names, nested placeholders and placeholders in host/user/password/options are refused. Missing database variables are allowed for non-PostgreSQL integrations. The plugin never connects, creates or deletes databases, or loads dotenv/credential files. Supply enough precreated worker databases for the requested count. Use the ordinary serial command without this plugin for -n 0; a shared DSN is never a parallel recipe.

Prepare the full integration prerequisites in the clean checkout after installing the frozen Python environment. These commands stage the data at the paths used by the tests and install the actual gateway package and Chromium; no external capture path or pi-ai environment variable is needed. Configure only the guarded disposable PostgreSQL routes described above, then run integration with the explicit browser flag:

    npm ci --prefix tests/node-gateway --ignore-scripts --no-audit --no-fund
    uv run python -m playwright install --with-deps chromium
    uv run python scripts/ci/stage_retained_fixtures.py
    uv run python tests/fixtures/supplementary-sources/stage.py
    export TANK_BROWSER_TESTS=1

The exact deployment-role SQL cases additionally require Docker and the postgres:16-bookworm image. They create their own labelled, network-isolated container and literal tank database on tmpfs, then remove only their owned container. They do not use the ordinary worker databases; missing prerequisites fail the selected run.

The browser marker requires TANK_BROWSER_TESTS=1 and local Chromium. The node marker may require a prepared upstream package tree. The historical marker requires retained capture inputs. The process marker identifies tests launching a local child executable. The CI integration job stages the exact retained market and supplementary source inputs, installs the locked actual pi-ai gateway package, and provisions its PostgreSQL prerequisites. It records the selected collection and requires every collected case to pass; any skip, failure, missing case or empty report fails CI. Actual integration deadline tests intentionally wait about 20 seconds per case.

## JavaScript and TypeScript

The standalone local lanes require Node 24 or later:

    node scripts/test-node.mjs local-unit
    node scripts/test-node.mjs local-contract

The runner stages generated fixtures in an owned temporary directory. The contract lane is separate from the local unit lane. On Node 24.22.0, the local unit lane passed all 43 cases in 1.134s (p90 0.012735s, maximum 0.131806s); the local contract lane passed all 21 cases in 0.566s. Historical frozen test assets are evidence, not duplicate current unit runs.

Prepared upstream TypeScript suites use Vitest 4.1.4 and Node 24.22.0. All eight prepared suites passed 71 cases with zero skips: serial wall time 22.459s, case p90 0.567288s, maximum 2.064464s. The run preserved all 75 original source hashes and used 1 CPU, 1.5 GiB, no swap, and an isolated network namespace. These package contracts do not qualify a VM.

Reproduction requires an isolated pinned OpenMA workcopy with the actual frozen dependencies and public package exports. Set OPENMA_ROOT to that tree, TARGET_TEST to one staged test path from node-lane-upstream-test-assets.json, AUDIT_CONFIG to the matching workspace-specific Vitest configuration, and REPORT to an owned output path. The verified invocation pattern is:

    cd "$OPENMA_ROOT"
    node "$OPENMA_ROOT/node_modules/vitest/vitest.mjs" run "$TARGET_TEST" --config "$AUDIT_CONFIG" --configLoader runner --reporter json --outputFile "$REPORT"

Run staged targets serially. The audit configuration uses absolute paths for its private workspace, so prepare a matching configuration for the new workcopy. The existing macOS Docker manifest is not a portable Linux staging command. Exact timings and frozen-source checks are retained in node-lane-upstream-final-timings.json and node-lane-upstream-final-results.json.

## Go assets

The active exchange collector is the Go module in capture/ (Go 1.25 or later in that release line). .github/workflows/capture.yml is its authoritative CI: go vet, go test -race with a migrated disposable PostgreSQL 16 database, recorded-frame/REST parity, generated-schema drift checks, and the selected Python reader suites. The ci.yml orchestrator calls capture.yml and test-suite.yml on every pull request and main push; the reusable workflows have no duplicate direct triggers. All Python, Node and Go lanes, source lint, format and mypy must pass before either SHA-tagged image is published.

The Python integration job also prepares Go and an offline module/build cache for the actual parity and PostgreSQL wrapper tests. Before a local full integration run, install the Go version required by capture/go.mod and prepare those caches once:

    export GOENV=off GOTOOLCHAIN=local
    export GOMODCACHE="$(go env GOMODCACHE)" GOCACHE="$(go env GOCACHE)" GOPATH="$(go env GOPATH)"
    mkdir -p capture/bin
    (cd capture && go mod download && CGO_ENABLED=0 go build -mod=readonly -o bin/capture-parity ./cmd/capture-parity)
    export GOPROXY=off GOSUMDB=off GOFLAGS=-mod=readonly

Explicit cache paths survive pytest's synthetic HOME/XDG settings. The Go child tests run without module or toolchain downloads. The parity subprocess is marked integration/process; the pure generated-schema comparison stays a unit. Missing Go/cache prerequisites fail the selected integration run.

Historical bridge audit:

The Go lane uses the standard testing package. All five top-level tests and ten table subtests passed offline with Go 1.25.14 and pinned gvisor-tap-vsock 0.8.7; every individual test took less than one second. The two package commands took 75.81s and 42.18s including initial compilation, separate from individual test timing. The reproducible receipt is evidence/go-lane-review.json. Running again requires an isolated prepared bridge, its pinned dependency tree, that Go toolchain, and a complete offline module cache.

In the prepared bridge:

    GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS='-mod=mod' go test -count=1 -timeout=20s -v -run '^TestTankReplay' ./...

In its prepared tank-vendor/gvisor-tap-vsock dependency:

    GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS='-mod=mod' go test -count=1 -timeout=20s -v -run '^TestTankReplay' ./pkg/services/dns

Run this lane with no external network and the normal available CPU and memory. The earlier cold-build measurements used a 1-CPU, 1-GiB audit allocation; that historical allocation is not the normal run command. The raw recipe directory is not a Go module. Handler tests using fake DNS response writers are units; they do not prove actual UDP/TCP exchange or native VM behavior. Keep immutable raw assets intact; the existing staging step applies gofmt.

## Common style

- Use pytest functions, ordinary fixtures, tmp_path, monkeypatch, direct assert, and pytest.raises. Async tests use the configured auto mode; new tests need no redundant asyncio decorator.
- Arrange a small explicit input, run the actual Tank behavior, and assert independently specified observable output or state. Prefer a focused regression name to a title claiming full production acceptance.
- Use httpx.MockTransport at an external HTTP boundary. Keep the domain function under test real. A real MemoryRepository is useful for a narrow service contract; composed workflows, actual databases, listeners, subprocesses, and browsers belong to integration selection.
- Use fixed timestamps or an injected clock. Coordinate concurrency with Events or explicit barriers. Short failure deadlines bound a test; arbitrary real sleeps should not simulate a clock.
- Keep test data minimal while crossing the actual boundary being protected. Preserve large-cardinality or retained-capture regressions as explicit integration cases rather than weakening their oracle.
- Assert outgoing request data, persisted effects, refusal behavior, and cleanup where relevant. A mock call count alone is inadequate unless the call itself is the external side-effect contract.
- Delete assertions about locally constructed values, copied constants, or a configured mock returning its configured result. Keep real settings precedence, validation, security, and compatibility contracts; they test project behavior.

Ruff owns Python formatting and imports:

    uv run --frozen --extra dev python scripts/ci/no_comments.py src tests scripts deploy capture
    uv run --frozen --extra dev ruff check src tests scripts deploy
    uv run --frozen --extra dev ruff format --check src tests scripts deploy
    uv run --frozen --extra dev mypy
    (cd capture && golangci-lint run ./...)

Retained verification trees also contain historical unittest.TestCase checks and before/after test copies. They are immutable QA evidence outside the active default suite; do not relabel them as fresh runs or rewrite those artifacts for style consistency.

## Remaining standardization plan

1. Keep non-migration PostgreSQL fixtures on the shared lazy template helper. Preserve explicit blank-database migration tests, per-worker administration databases, and owned cleanup; new fixtures should reuse this lifecycle instead of copying schema bootstrap.
2. Split large mixed test files by event persistence, publication, replay, and repository contracts, preserving the ordinary pytest marks and independent regression oracles.
3. Extract genuinely repeated small builders into narrow test helpers. Keep case-specific evidence explicit rather than growing a single configurable fixture factory.
4. Finish a separate mechanical import/formatter pass and normalize remaining redundant async decorators and legacy class style. `.github/workflows/test-suite.yml` now runs three separate jobs: Python units with the JUnit timing budget, Python integration, and Node local units/contracts. The integration checker requires the exact collected case set and rejects every skip, failure or error.
5. Standardize current standalone JavaScript tests on node:test and node:assert/strict; retain Vitest where upstream package composition requires it. Migrate real-timer cancellation cases to injected clocks while preserving their cleanup and failure behavior.

The cleanup establishes commands and conventions. A green result from the current CI qualifies its collected Python, Go and Node checks. Historical upstream checks, native VM verification and production acceptance are separate qualifications; historical assets may retain different styles.

## CI images and manual deployment

The pinned CI environment uses uv 0.11.7, Python 3.13.13, Node 24.22.0 and Go 1.25.14. Ruff, mypy and test tools are pinned in pyproject.toml and uv.lock; golangci-lint 2.5.0 is verified against its release archive SHA256. The source comment scanner checks Python tokens and Go lexical comments while preserving semantic directives. It excludes immutable history, upstream/vendor trees and retained fixture assets.

Every push to main builds and publishes the five images ([DOCKER_BUILDS.md](DOCKER_BUILDS.md)), tagged by the full commit SHA. deploy.yml is workflow_dispatch only, runs only on main (any other branch, or a commit not on main, fails) and uses the production environment with the secrets PROD_SSH_USER, PROD_SSH_HOST, PROD_SSH_KEY and PROD_SSH_KNOWN_HOSTS. It waits for the commit's CI run, then runs `scripts/deploy/deploy.sh` on the host ([OPERATIONS.md](OPERATIONS.md#deployment)). CI work does not execute deployment.

## ClickHouse integration contracts

The full Python integration job and the Go race job each receive a separate dedicated ClickHouse 25.8 service. Set CLICKHOUSE_TEST_URL to that caller-owned loopback HTTP service and initialize its eight market/trade tables with `uv run --frozen --extra dev python scripts/ci/prepare_clickhouse.py`. Never supply a production ClickHouse endpoint. The bootstrap rejects non-loopback URLs and other server release lines.

Python integration uses `-n auto --dist loadfile`; the two ClickHouse cases share one file and reset the mkt database, so the file stays on one worker while other integration files use the available workers. Both cases must pass the exact-collection zero-skip gate. Go emits its race report as JSON and scripts/ci/check_go_results.py requires every started case to pass with zero skips. The Go store and raw-trade tests use disjoint tables in their own job's ClickHouse instance.
