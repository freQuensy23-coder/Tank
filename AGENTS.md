# Tank — agent instructions

Specifications live in `01_Tank_V0_PRD.md`, `02_Tank_V0_HLD.md`, `03_Tank_V0_Technical_Design.md` and `docs/`. This file holds only working rules.

## Working environment

- Develop and run tests in a local checkout. Servers are not development environments.
- Do not access the user's personal Chrome.
- Keep secrets out of source control, logs and command output.
- Do not stop, clean up or redeploy another agent's or an unidentified process or container.

## Code

- Add no ordinary comments to active Python or Go source, tests, scripts, deployment helpers or capture code. Docstrings are allowed. Keep executable shebangs, Go compiler/build/line directives, cgo preambles and required literal integration patch data.
- Preserve upstream and vendored third-party source, LICENSE/copyright notices, immutable history, retained fixture payloads and verification evidence.
- Use modern Python syntax and absolute imports. Do not restore the retired Python exchange producer or legacy compatibility code.
- Preserve public Go error strings covered by parity contracts.
- Run Ruff check, Ruff format and mypy against the current source.

## Tests

- Use the toolchain pinned in CI and the frozen `uv.lock`.
- Follow [docs/TESTING.md](docs/TESTING.md). Unit tests select `not integration`, have zero skips, take under 10 s per case and keep p90 under 1 s. Integration runs separately against real services and must pass its full collected set with zero skips.
- Run pytest with `-n auto`. Do not add resource quotas, worker caps or CPU affinity.

## Deployment

- Production is `oracle-vps` ([docs/OPERATIONS.md](docs/OPERATIONS.md)).
- Deploy only with the GitHub Actions `deploy` workflow (`.github/workflows/deploy.yml`), started by hand from `main`. Do not start it during CI maintenance.
- Never deploy over SSH: no `git clone`, `pull` or `checkout` on a server, no copying code, compose files or images to it, no manual `docker compose` up/pull/restart.
- SSH to production is for read-only diagnostics. The only host-side change is the deployment owner editing secrets in `/srv/tank-deploy/.env`.

## Pull requests

- Write every PR description in English from `.github/pull_request_template.md`. The `pr-description` CI check fails when its blocks are missing or out of order.
- Make the before, after and red diff images of a changed diagram with `scripts/diagram_diff.sh <base-ref> <diagram.svg> <out-dir>`.
- After a database schema change, regenerate the schema diagrams with `uv run python scripts/render_db_schema.py --png`.

## Code Review Rules

### Required values, typing and data contracts

- Review changed functions, helpers, callbacks and their real callers. Require parameter and return annotations and precise collection element types. A green mypy run is not evidence that untyped bodies were checked.
- Flag defaults that conceal a broken required contract: `value or ""`, `record.get("required_field")`, zero/empty-container substitutes, guessed identities, or `T | None = None` when the operation requires a value. Validate required facts at their admission boundary and carry them as required typed fields.
- Keep genuine absence explicit. Unknown, unrequested, unavailable, not yet occurred, empty and known zero are different states. Use phase-specific models when a field is required only in one phase; do not require an unused historical field just to finalize another phase.
- A retained fallback or optional field needs an MR explanation naming the symbol, the actual contract that permits absence, the behavior on absence, and its relevant regression coverage. Declared configuration defaults, optional protocol fields and associative lookups can be legitimate; a broad “legacy” or “defensive” label is insufficient.
- Reject `Any`, bare containers and opaque substitutes for known domain values. `object`, arbitrary `JsonValue`, generic JSON wrappers, casts, ignores and disabled checks must not conceal an unfinished model or an incompatible caller. Raw unknown values are appropriate at actual external admission boundaries and must be narrowed before domain use.
- Use named strict Pydantic records for structured domain/application data. Validate required fields, unknown fields, finite numbers, boolean/integer distinctions and cross-field invariants. An annotation or unchecked `model_copy`/`model_construct` is not runtime validation.
- Keep dictionary-shaped data at justified boundaries: raw provider JSON, driver/HTTP serialization, headers, and indexes keyed by real dynamic identifiers with precise value types. Explain retained mappings in the MR. Moving the same anonymous payload into a Pydantic field does not establish a domain contract.
- Check serialization in both directions and follow the actual serializer used by callers. Reject ambiguous duplicate keys before projecting structured JSON into facts; preserve opaque text as text. Preserve exact raw evidence, numeric precision, field-presence semantics and stable identities where those are part of the contract.

### Architecture and evidence

- Follow dependencies across contracts, pure domain policy/calculation, application orchestration, persistence/network adapters and presentation. Flag domain or contract modules that import an outward service, perform I/O, render transport wording or decide deployment policy.
- Keep policy decisions in the layer that owns them and make dependencies explicit through narrow typed interfaces. Moving a mixed module or adding a wrapper does not fix a dependency inversion if the same outward imports and responsibilities remain.
- Inspect the state-changing callers, not only new DTOs: database writes, queueing, cursor advancement, external requests, retries and delivery acknowledgments. Required ownership and scope checks must hold through the actual transaction or lock; do not assume an advisory lock protects writers that never acquire it.
- Preserve the provenance of retained observations. Do not fill historical gaps with current configuration, collection time, another record's identity, an array position or a different clock. Distinguish native-quote quantities from USD values and source observations from derived projections. Validate data consumed by the current phase without inventing unrelated facts.

### Tests and review evidence

- Require tests that can fail for a real behavioral regression. Prefer an independently specified result, an actual boundary/caller, and observable state or effects. Flag tautologies, self-comparisons, empty tests, tests of the mock's own return value, and mocks that replace the behavior claimed as tested.
- Exercise relevant failure paths as well as positive controls: missing/invalid required facts, absence versus zero, rejected writes or cursor advances, identity mismatch, partial failure, retry/restart and concurrency when the change touches them. Do not weaken deadlines, skip failures, fabricate data or rewrite immutable fixtures to make a regression disappear.
- When adapting a fixture, explain which real contract it represents and retain its original behavioral assertions. A failure may reveal product overvalidation or a broken caller; adding dummy fields is not automatically the fix.
- Match claims to the exact tested commit, environment and complete collected case set. Distinguish model-only, mocked-boundary, real-service and image/runtime proof. Historical green CI, a strict subset, or passing constructor tests does not qualify a changed application.

### Where the reviewer should look

- Start with the affected PRD/HLD/technical-specification sections, the request/response or storage contract, and both producers and consumers. Inspect `src/tank/contracts/`, adjacent `*_contracts.py`/`*_codec.py` modules, and the actual HTTP, JSON, SQL and filesystem adapters.
- For responsibility and exception context, read [the architecture audit](docs/refactor/architecture.md), [executable boundaries](docs/refactor/executable-boundaries.md) and the relevant area report: [foundation](docs/refactor/foundation.md), [analysis](docs/refactor/analysis.md), [agents](docs/refactor/agents.md), [ingestion](docs/refactor/ingestion.md) or [quality](docs/refactor/quality.md). These ledgers record evidence and unfinished work; they are not blanket exemptions from the current contract.
- Inspect the actual CI commands and type-check scope in `.github/workflows/`, the relevant assertions and fixtures under `tests/`, and the changed migrations and generated schema diagrams. A grep match is a review candidate, not proof of a defect.
- Report concrete violations with the affected location, trigger, expected behavior, observed consequence and relevant contract. Apply these rules to changed code and necessary downstream contracts; do not turn an unrelated MR into a whole-repository cleanup or demand rewrites of protected third-party source.

### Code style

- Check changed code against the Code rules above, including the ordinary-comment prohibition and its exceptions, modern Python syntax, absolute imports, preserved public Go error strings, and protection of upstream/vendor source and retained evidence. Use the configured Ruff, formatting, mypy and Go CI results for mechanical checks; flag policy violations that those checks miss. Do not demand style-only rewrites of untouched code or protected third-party and evidence trees.

### One MR, one feature

- Each MR (GitHub pull request) must deliver one cohesive feature, fix or maintenance objective. Flag unrelated features, fixes, refactors or cleanup bundled into the same MR and ask for separate MRs. Supporting code, tests, migrations, configuration and specification updates belong together when they are necessary for that single objective; a cross-file change alone is not a violation.

### Product, technical specifications and implementation

- Check that the changed behavior, product specification (`01_Tank_V0_PRD.md`), architecture (`02_Tank_V0_HLD.md`), technical specification (`03_Tank_V0_Technical_Design.md`) and affected code, configuration, schemas and tests agree. Follow relevant callers and consumers beyond the diff to identify broken contracts. Flag implementation that contradicts the specifications, specification changes without the required implementation, and behavioral changes whose specifications were not updated. Cite the relevant specification section and affected implementation in each finding. The safe path is to preserve the existing specified behavior or update the affected specifications and implementation together for the MR's single objective; do not report unrelated pre-existing drift as introduced by the MR.
