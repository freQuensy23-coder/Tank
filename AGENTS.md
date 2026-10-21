# User instructions — Tank

Updated 2026-10-20, from the human user in the original Tank conversation.

- The user has appointed a second agent as the lead for this same task. That agent's changes and architectural decisions take precedence over the original agent and all of its subagents.
- Original-agent followers must stop overlapping edits until ownership is coordinated. Do not overwrite, revert, reformat, or replace the lead agent's changes with an older copy. Preserve existing evidence; supply read-only analysis and handoff information.
- The lead agent may continue the user's work. These coordination instructions do not require the lead to obtain approval from the original agent.
- Do not stop, clean up, or redeploy another agent's or an unidentified process/container.
- The objective is to implement the system and run it in production on openclaw-dp. Development, builds, tests, and runtime belong on that server, in /srv/tank.
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
- CI publishes qualified Tank and Go capture images after every required lane passes on main/prod-main. Production deployment is manual and requires the matching clean /srv/tank checkout; do not invoke it while doing CI maintenance.
