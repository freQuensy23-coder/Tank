# Tank V0 — Major Event Hunter

The main branch contains the specifications and production code together. CI builds images from main; the manual Deploy workflow deploys a qualified commit from that branch.

Tank collects public information and market data, maintains one OpenMA analysis session per event, computes market assessments and long-position guidance in Python, and delivers evidence-backed decision cards to Telegram. It has no exchange trading credentials or order execution path. The revised deployment uses three Linux Docker Compose services: Tank, OpenMA and PostgreSQL 16. Tank's internal Codex gateway connects OpenMA to the operator's ChatGPT subscription; there are no local token or spending caps.

Tank filtering and event routing use Microsoft-Decision-1 version 1 through Azure Foundry, as requested on 2026-10-25. The service posts typed `state`/`questions` to the resource's `/providers/microsoft/v1/systemone` endpoint with `api-key` authentication and deployment name `decision-1`. Analyst/scout models continue through the Codex subscription gateway. See [Foundry setup](docs/FOUNDRY_DECISION.md). Current acceptance evidence is tracked in [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md).

## Development and tests

```sh
uv sync --python 3.12 --frozen --extra dev
uv run tank --help
uv run tank doctor
uv run pytest -q -n auto --dist loadfile
```

Run development commands in an isolated checkout on the server. The pytest command runs the offline unit selection across available CPUs with a 10-second per-case timeout. Normal test runs use no artificial CPU or memory quotas. [Testing conventions and separate integration commands](docs/TESTING.md) include the p90 <1s check, resource prerequisites, and Node lanes.

`doctor` reports missing deployment configuration without printing credentials. Credentials remain outside the repository. Filter and routing cutoffs remain the user-selected **uncalibrated 0.5 midpoint**; Decision-1 needs its own labeled calibration.

For runs outside Compose, set `TANK_FOUNDRY_API_KEY` (or `FOUNDRY_API_KEY`), `TANK_FOUNDRY_ENDPOINT` and `TANK_FOUNDRY_DEPLOYMENT`; `.env.example` gives the defaults. Codex authorization defaults to `$CODEX_HOME/auth.json` when set, otherwise `~/.codex/auth.json`; `TANK_CODEX_AUTH_PATH` overrides it. Compose uses its private `/codex-auth/auth.json` volume for the analyst.

For database tests, use isolated PostgreSQL 16 databases. Test databases must never contain production data:

```sh
TANK_TEST_DATABASE_URL=postgresql:///tank_test \
TANK_E2E_DATABASE_URL=postgresql:///tank_test_e2e \
uv run pytest -q -m 'integration and postgres' --timeout=300
```

For parallel integration, precreate separate worker databases and use the opt-in portable plugin recipe in [Testing](docs/TESTING.md). Do not share the serial DSNs across xdist workers.

The end-to-end tests use the real repository, job workers, source parser, session/tool bridge, market calculations, agent-authored event persistence, card composition, and delivery ledger. External source, model and Telegram responses are explicitly mocked. Public adapter smoke checks, live source latency, model quality, and unattended acceptance are recorded separately.

Pytest disables operator dotenv files, credential paths and external Python network access. [The offline test policy](docs/verification/offline-policy.md) explains its boundaries and the explicit opt-ins for disposable databases and upstream OpenMA probes. Static checks use `uv run ruff check --select E4,E7,E9,F,I src tests deploy` and `uv run ruff format --check src tests deploy`.

## Service modules

| Module | Responsibility |
|---|---|
| `sources` | RSS/Atom/REST/GraphQL/HTML ingestion and the Apify X actor; raw evidence and source checkpoints |
| `agents` | Decision-1 filtering/routing and durable OpenMA sessions with tool-result recovery |
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

Follow [the deployment runbook](docs/OPERATIONS.md). The existing 0.5 midpoint remains explicitly uncalibrated; use `tank calibrate` on labeled data to propose Decision-1 cutoffs. `serve` is the service command. Operator commands include `doctor`, `db migrate`, `sources check`, `coverage`, `archive fetch`, `review list/show/label/metrics`, `reaction`, `scout list/decide`, `replay`, `bundle reproduce`, `backup`, and `calibrate`; each exposes arguments through `--help`.

Replay requires a fresh, isolated database. Calibration produces a report and proposed threshold changes; it does not apply thresholds automatically. The saved historical fixture checks retain their original source distinctions, including REST open-interest snapshots and archive-based historical distributions.

## Acceptance status

The specifications require more than local tests: 72 hours of source operation, labeled routing/calibration evidence, complete fixture packets, a live source-to-Telegram run, and 14 days of shadow review. These gates remain visible in the implementation ledger. Current F-16 expects STRESS, matching Technical Design §5.7; the rule was not adjusted to fit a test. The removed daily-budget policy is absent from runtime behavior, while historical records remain readable.

Development verification uses synthetic credentials, mock provider responses, real local HTTP listeners, PostgreSQL and isolated containers. It does not read the operator's sign-in or make authenticated external calls. [The requirements delta](docs/verification/requirements-delta.json) maps the revised specifications to implementation and tests; [the gateway notes](docs/CODEX_GATEWAY.md) record the verified protocol and integration limits.
