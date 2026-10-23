# Tank V0 — Major Event Hunter

Tank collects public information and market data, maintains one OpenMA analysis session per event, computes market assessments and long-position guidance in Python, and delivers evidence-backed decision cards to Telegram. It has no exchange trading credentials or order execution path. The revised deployment uses three Linux Docker Compose services: Tank, OpenMA and PostgreSQL 16. Tank's internal Codex gateway connects OpenMA to the operator's ChatGPT subscription; there are no local token or spending caps.

The implementation follows the three specifications in this directory and recorded operator changes. On 18 October 2026 the operator selected OpenRouter for Jev, using the existing OpenRouter key and plain HTTP with the documented System One endpoint and namespaced `typesafe/jev-1.13` model. Analysis remains on the Codex subscription gateway. Current acceptance evidence and unresolved specification conflicts are tracked in [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md). Operational installation is described in [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Development and tests

```sh
uv sync --python 3.12 --frozen --extra dev
uv run tank --help
uv run tank doctor
uv run pytest -q -n auto --dist loadfile
```

Run development commands in an isolated checkout on the server. The pytest command runs the offline unit selection across available CPUs with a 10-second per-case timeout. Normal test runs use no artificial CPU or memory quotas. [Testing conventions and separate integration commands](docs/TESTING.md) include the p90 <1s check, resource prerequisites, and Node lanes.

`doctor` reports missing deployment configuration without printing credentials. Credentials remain outside the repository. The local configuration records deployed OpenMA resource IDs and the operator-approved Jev baseline: filter 0.23, routing 0.67. Its measured precision/recall and outstanding acceptance gates are recorded in the implementation ledger.

For local runs, `OPENROUTER_API_KEY` or `TANK_OPENROUTER_API_KEY` supplies the default Jev provider. Codex authorization defaults to `$CODEX_HOME/auth.json` when set, otherwise `~/.codex/auth.json`; `TANK_CODEX_AUTH_PATH` overrides that location. Compose explicitly uses its private `/codex-auth/auth.json` volume and `openrouter_api_key` secret. OpenRouter `/api/v1/systemone` is the sole Jev route; no direct TypeSafe SDK, credential alternative or provider fallback is supported.

For database tests, use isolated PostgreSQL 16 databases. Test databases must never contain production data:

```sh
TANK_TEST_DATABASE_URL=postgresql:///tank_test \
TANK_E2E_DATABASE_URL=postgresql:///tank_test_e2e \
uv run pytest -q -m 'integration and postgres' --timeout=300
```

For parallel integration, precreate separate worker databases and use the opt-in portable plugin recipe in [Testing](docs/TESTING.md). Do not share the serial DSNs across xdist workers.

The end-to-end tests use the real repository, job workers, source parser, session/tool bridge, market calculations, agent-authored event persistence, card composition, and delivery ledger. External source, model and Telegram responses are explicitly mocked. Public adapter smoke checks, live source latency, model quality, and unattended acceptance are recorded separately.

Pytest disables operator dotenv files, credential paths and external Python network access. [The offline test policy](docs/verification/offline-policy.md) explains its boundaries and the explicit opt-ins for disposable databases, Chromium and upstream OpenMA probes. Static checks use `uv run ruff check --select E4,E7,E9,F,I src tests deploy` and `uv run ruff format --check src tests deploy`.

## Service modules

| Module | Responsibility |
|---|---|
| `sources` | RSS/Atom/REST/GraphQL/HTML and Playwright ingestion; raw evidence and source checkpoints |
| `agents` | Jev filtering/routing and durable OpenMA sessions with tool-result recovery |
| `codex_gateway` | Internal Responses endpoint, subscription authentication, atomic OAuth refresh and streaming |
| `event_tools` | Persistence of agent claims and judgments, registry identity resolution, beneficiary records and publication gate |
| `market` | Venue discovery, REST/WS capture, normalization, archives and fundamentals |
| `analysis` | Market Truth, confidence, outlook, grades, actions, card composition |
| `storage` | PostgreSQL migrations, immutable records and durable jobs; market series and prints routed to ClickHouse (`clickhouse_market`) |
| `runtime`, `scheduler`, `delivery` | Eight job workers, conditions, milestones, reactions, operations and Telegram delivery |
| `replay`, `calibration` | Isolated replay and reviewed threshold calibration |
| `review` | Frozen evidence, horizon outcomes, versioned labels and S1–S9 with explicit coverage |

The service and gateway use one asyncio process. tank-capture, PostgreSQL, ClickHouse and OpenMA run beside it in separate containers. CPU-heavy analysis runs outside the ingestion event loop. Module interfaces and ownership are recorded in [docs/IMPLEMENTATION_CONTRACT.md](docs/IMPLEMENTATION_CONTRACT.md). Secrets (the host `.env`) and the subscription sign-in stay outside versioned configuration and evidence artifacts.

## Operator commands

Follow [the deployment runbook](docs/OPERATIONS.md). Run `tank calibrate` offline and review both Jev cutoffs before the first `tank serve` startup. `serve` is the service command. Operator commands include `doctor`, `db migrate`, `sources check`, `coverage`, `archive fetch`, `review list/show/label/metrics`, `reaction`, `scout list/decide`, `replay`, `bundle reproduce`, `backup`, and `calibrate`; each exposes arguments through `--help`.

Replay requires a fresh, isolated database. Calibration produces a report and proposed threshold changes; it does not apply thresholds automatically. The saved historical fixture checks retain their original source distinctions, including REST open-interest snapshots and archive-based historical distributions.

## Acceptance status

The specifications require more than local tests: 72 hours of source operation, labeled routing/calibration evidence, complete fixture packets, a live source-to-Telegram run, and 14 days of shadow review. These gates remain visible in the implementation ledger. Current F-16 expects STRESS, matching Technical Design §5.7; the rule was not adjusted to fit a test. The removed daily-budget policy is absent from runtime behavior, while historical records remain readable.

Development verification uses synthetic credentials, mock provider responses, real local HTTP listeners, PostgreSQL, Chromium and isolated containers. It does not read the operator's sign-in or make authenticated external calls. [The requirements delta](docs/verification/requirements-delta.json) maps the revised specifications to implementation and tests; [the gateway notes](docs/CODEX_GATEWAY.md) record the verified protocol and integration limits.
