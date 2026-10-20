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
