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

### Code style

- Check changed code against the Code rules above, including the ordinary-comment prohibition and its exceptions, modern Python syntax, absolute imports, preserved public Go error strings, and protection of upstream/vendor source and retained evidence. Use the configured Ruff, formatting, mypy and Go CI results for mechanical checks; flag policy violations that those checks miss. Do not demand style-only rewrites of untouched code or protected third-party and evidence trees.

### One MR, one feature

- Each MR (GitHub pull request) must deliver one cohesive feature, fix or maintenance objective. Flag unrelated features, fixes, refactors or cleanup bundled into the same MR and ask for separate MRs. Supporting code, tests, migrations, configuration and specification updates belong together when they are necessary for that single objective; a cross-file change alone is not a violation.

### Product, technical specifications and implementation

- Check that the changed behavior, product specification (`01_Tank_V0_PRD.md`), architecture (`02_Tank_V0_HLD.md`), technical specification (`03_Tank_V0_Technical_Design.md`) and affected code, configuration, schemas and tests agree. Follow relevant callers and consumers beyond the diff to identify broken contracts. Flag implementation that contradicts the specifications, specification changes without the required implementation, and behavioral changes whose specifications were not updated. Cite the relevant specification section and affected implementation in each finding. The safe path is to preserve the existing specified behavior or update the affected specifications and implementation together for the MR's single objective; do not report unrelated pre-existing drift as introduced by the MR.
