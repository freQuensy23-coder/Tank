# User instructions — Tank

Updated 2026-10-20, from the human user in the original Tank conversation.

- The user has appointed a second agent as the lead for this same task. That agent's changes and architectural decisions take precedence over the original agent and all of its subagents.
- Original-agent followers must stop overlapping edits until ownership is coordinated. Do not overwrite, revert, reformat, or replace the lead agent's changes with an older copy. Preserve existing evidence; supply read-only analysis and handoff information.
- The lead agent may continue the user's work. These coordination instructions do not require the lead to obtain approval from the original agent.
- Do not stop, clean up, or redeploy another agent's or an unidentified process/container.
- Do not access the user's personal Chrome. X authorization was supplied as /Users/a.mametyev/.config/tank/x-session.json; preserve the original and keep secrets out of output.

Read-only original-agent handoff: `/srv/tank/data/qa/original-agent-handoff-20261020T162237Z/snapshot.json`. It records current evidence and unfinished work; it does not override the lead agent.

## Lead coordination

The rules, workstreams and file ownership of the lead session are in /srv/tank-work/COORDINATION.md. Original-agent followers stay read-only: do not edit /srv/tank or /srv/tank-work.

## CI and active source policy

- In an explicitly authorized isolated CI worktree, use Python 3.13.13 and the frozen uv.lock with uv 0.11.7. Production work remains subject to the lead coordination rules above.
- Add no ordinary comments to active Python or Go source, tests, scripts, deployment helpers, or capture code. Docstrings are allowed. Preserve executable shebangs, Go compiler/build/line directives, cgo preambles, and required literal integration patch data.
- Preserve upstream and vendored third-party source, LICENSE/copyright notices, immutable history, retained fixture payloads, and verification evidence. The source policy checker and Ruff explicitly exclude those trees.
- Use modern Python syntax and absolute imports. Do not restore the retired Python exchange producer or legacy compatibility implementations.
- Run Ruff checks, Ruff formatting, and mypy against the current source. Preserve public Go error strings covered by parity contracts; .golangci.yml narrowly exempts their exact ST1005 source expressions.
- Unit tests select `not integration`, must have zero skips, take less than 10 seconds per case including fixtures, and have p90 below 1 second. Run integration separately with its actual services and require the full collected case set to pass with zero skips.
- Use pytest-xdist `-n auto` and normal available CPU/memory. Do not introduce artificial resource quotas, worker caps, or CPU affinity restrictions.
- CI builds and pushes the five images by commit SHA in parallel with the tests; only a commit whose whole CI run passed gets the `main` tag and can be deployed. Production deployment is the manual Deploy workflow, from main only; do not invoke it while doing CI maintenance.

## Deployment

- SSH access to the production server is not for deploys. Never deploy over SSH: no manual `docker compose` up/pull/restart, image changes or checkout updates in /srv/tank.
- Every production deploy runs through the GitHub Actions `deploy` workflow (`.github/workflows/deploy.yml`), dispatched from the remote `main` branch.

## Code Review Rules

### Code style

- Check changed code against the CI and active source policy above, including the ordinary-comment prohibition and its exceptions, modern Python syntax, absolute imports, preserved public Go error strings, and protection of upstream/vendor source and retained evidence. Use the configured Ruff, formatting, mypy and Go CI results for mechanical checks; flag policy violations that those checks miss. Do not demand style-only rewrites of untouched code or protected third-party and evidence trees.

### One MR, one feature

- Each MR (GitHub pull request) must deliver one cohesive feature, fix or maintenance objective. Flag unrelated features, fixes, refactors or cleanup bundled into the same MR and ask for separate MRs. Supporting code, tests, migrations, configuration and specification updates belong together when they are necessary for that single objective; a cross-file change alone is not a violation.

### Product, technical specifications and implementation

- Check that the changed behavior, product specification (`01_Tank_V0_PRD.md`), architecture (`02_Tank_V0_HLD.md`), technical specification (`03_Tank_V0_Technical_Design.md`) and affected code, configuration, schemas and tests agree. Follow relevant callers and consumers beyond the diff to identify broken contracts. Flag implementation that contradicts the specifications, specification changes without the required implementation, and behavioral changes whose specifications were not updated. Cite the relevant specification section and affected implementation in each finding. The safe path is to preserve the existing specified behavior or update the affected specifications and implementation together for the MR's single objective; do not report unrelated pre-existing drift as introduced by the MR.
