# Tank V0 — Major Event Hunter

Tank collects public information and market data, maintains one OpenMA analysis session per event, computes market assessments and long-position guidance in Python, and delivers evidence-backed decision cards to Telegram. It has no exchange trading credentials or order execution path. The revised deployment uses three Linux Docker Compose services: Tank, OpenMA and PostgreSQL 16. Tank's internal Codex gateway connects OpenMA to the operator's ChatGPT subscription; there are no local token or spending caps.

The implementation follows the three specifications in this directory and recorded operator changes. On 18 October 2026 the operator selected OpenRouter for Jev, using the existing OpenRouter key and the official TypeSafe SDK's compatible System One endpoint. Analysis remains on the Codex subscription gateway. Current acceptance evidence and unresolved specification conflicts are tracked in [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md). Operational installation is described in [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Local development

```sh
uv sync --python 3.12 --frozen --extra dev
uv run tank --help
uv run tank doctor
uv run pytest -q
```

`doctor` reports missing deployment configuration without printing credentials. The checked-in configuration intentionally leaves operator credentials, deployed OpenMA resource IDs, and experimentally calibrated Jev cutoffs unset.

For local runs, `OPENROUTER_API_KEY` or `TANK_OPENROUTER_API_KEY` supplies the default Jev provider. Codex authorization defaults to `$CODEX_HOME/auth.json` when set, otherwise `~/.codex/auth.json`; `TANK_CODEX_AUTH_PATH` overrides that location. Compose explicitly uses its private `/codex-auth/auth.json` volume and `openrouter_api_key` secret. Direct TypeSafe remains an explicit alternative selected with `TANK_JEV_PROVIDER=typesafe`; credentials never trigger a silent provider fallback.

For database tests, use isolated PostgreSQL 16 databases. Test databases must never contain production data:

```sh
TANK_TEST_DATABASE_URL=postgresql:///tank_test \
TANK_E2E_DATABASE_URL=postgresql:///tank_e2e \
uv run pytest -q
```

The end-to-end tests use the real repository, job workers, source parser, session/tool bridge, market calculations, event validation, card linter, and delivery ledger. External source, model and Telegram responses are explicitly mocked. Public adapter smoke checks, live source latency, model quality, and unattended acceptance are recorded separately.

Pytest disables operator dotenv files, credential paths and external Python network access. [The offline test policy](docs/verification/offline-policy.md) explains its boundaries and the explicit opt-ins for disposable databases, Chromium and upstream OpenMA probes. Static checks use `uv run ruff check --select E4,E7,E9,F,I src tests deploy` and `uv run ruff format --check src tests deploy`.

## Service modules

| Module | Responsibility |
|---|---|
| `sources` | RSS/Atom/REST/GraphQL/HTML and Playwright ingestion; raw evidence and source checkpoints |
| `agents` | Jev filtering/routing and durable OpenMA sessions with tool-result recovery |
| `codex_gateway` | Internal Responses endpoint, subscription authentication, atomic OAuth refresh and streaming |
| `event_tools` | Quoted evidence, event states, assessments, beneficiaries and publication validation |
| `market` | Venue discovery, REST/WS capture, normalization, archives and fundamentals |
| `analysis` | Market Truth, confidence, outlook, grades, actions, card composition and linter |
| `storage` | PostgreSQL migrations, immutable records, partitioned market tables and durable jobs |
| `runtime`, `scheduler`, `delivery` | Eight job workers, conditions, milestones, reactions, operations and Telegram delivery |
| `replay`, `calibration` | Isolated replay and reviewed threshold calibration |
| `review` | Frozen evidence, horizon outcomes, versioned labels and S1–S9 with explicit coverage |

The service and gateway use one asyncio process. PostgreSQL and OpenMA run beside it in separate containers. CPU-heavy analysis runs outside the ingestion event loop. Module interfaces and ownership are recorded in [docs/IMPLEMENTATION_CONTRACT.md](docs/IMPLEMENTATION_CONTRACT.md). Docker secrets and the subscription sign-in stay outside versioned configuration and evidence artifacts.

## Operator commands

Follow [the deployment runbook](docs/OPERATIONS.md), then use `docker compose exec tank tank …` for operator commands. Commands include `doctor`, `capture`, `db migrate`, `sources check`, `coverage`, `archive fetch`, `review list/show/label/metrics`, `reaction`, `scout list/decide`, `replay`, `bundle reproduce`, `backup`, and `calibrate`. Every command exposes its arguments through `--help`. [Capture mode](docs/CAPTURE.md) collects V0-A evidence before event-model calibration; `serve` continues to require measured cutoffs.

Replay requires a fresh, isolated database. Calibration produces a report and proposed threshold changes; it does not apply thresholds automatically. The saved historical fixture checks retain their original source distinctions, including REST open-interest snapshots and archive-based historical distributions.

## Acceptance status

The specifications require more than local tests: 72 hours of source operation, labeled routing/calibration evidence, complete fixture packets, a live source-to-Telegram run, and 14 days of shadow review. These gates remain visible in the implementation ledger. Current F-16 expects STRESS, matching Technical Design §5.7; the rule was not adjusted to fit a test. The removed daily-budget policy is absent from runtime behavior, while historical records remain readable.

Development verification uses synthetic credentials, mock provider responses, real local HTTP listeners, PostgreSQL, Chromium and isolated containers. It does not read the operator's sign-in or make authenticated external calls. [The requirements delta](docs/verification/requirements-delta.json) maps the revised specifications to implementation and tests; [the gateway notes](docs/CODEX_GATEWAY.md) record the verified protocol and integration limits.
