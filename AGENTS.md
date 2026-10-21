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

## Code rules (enforced by CI, `.github/workflows/ci.yml`)

- **No comments.** No `#` comments in Python and no `//` or `/* */` in Go, except `//go:` directives. Name things so the code explains itself. Docstrings only where an API needs them. `scripts/ci/no_comments.py` fails the build.
- **No legacy.** Python 3.13+ only. No `from __future__ import annotations`, no `typing.Optional/Union/List/Dict/Tuple/Set` (use `X | None`, `list[...]`), no relative imports, no compatibility shims, no dead or commented-out code. Delete what you replace in the same change.
- **Green before merge.** These must pass: `ruff check`, `ruff format --check`, `mypy`, and for Go `gofmt`, `go vet` and `golangci-lint`. Suppressing a finding in code is not allowed; fix it.
- **Tests come with every change.** Unit tests are fast and deterministic: no network, no real sleeps or wall-clock waits (inject the clock and timeouts), and no files outside the repo. The whole unit run (`pytest -m "not slow"`) must stay under 3 min on a 4-core runner. A test that needs real time or heavy end-to-end data gets `@pytest.mark.slow`, and that marker is the exception, not the default.
- **Deploy** only through the `deploy` workflow (image from GHCR built by CI), never by hand from a dirty checkout.
