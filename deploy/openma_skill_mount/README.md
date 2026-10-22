# Inline skill mount (node runtime)

HLD 02 §5: OpenMA inlines the agents' skills into the system prompt at session start.
The pinned node runtime builds the harness agent from the session's agent snapshot
(`toLegacyHarnessAgentConfig`), which has no `metadata`; the harness reads the skill
mount mode from `agent.metadata.skill_mount`, so it always fell back to `progressive`
(skill name and SKILL.md path only, read on demand).

This stage changes one line of `packages/control-plane-node/src/lib/node-managed-agent-codec.ts`:
the harness agent config carries `metadata: { skill_mount: "inline" }`. Both consumers of the
codec (skill mount resolution, which then fetches the SKILL.md bodies, and the pi adapter's
exposure plan) see the inline mode. Guarded by the exact upstream and output hashes in
`source-guards.json`; applied after `openma_tool_kind` by `prepare_openma_deployment.py`.
