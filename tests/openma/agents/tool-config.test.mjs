import { test } from 'node:test';
import assert from 'node:assert/strict';
import { toAgentToolInput, fromAgentToolInput } from '../../../vendor/openma/packages/managed-agents-api/src/mappers/agent-definition.ts';
import { resolveAgentTools } from '../../../vendor/openma/packages/managed-agents-application/src/agents/definition-resolution.ts';

for (const name of ['bash', 'edit', 'read', 'write', 'glob', 'grep', 'browser', 'schedule', 'cancel_schedule', 'list_schedules']) {
  test(`${name} maps and resolves matching tool discriminants without losing permission overrides`, () => {
    const input = toAgentToolInput({
      type: 'agent_toolset_20260401',
      default_config: { enabled: false, permission_policy: { type: 'always_ask' } },
      configs: [
        { name, type: name, enabled: true, permission_policy: { type: 'always_allow' } },
        { name, enabled: null, permission_policy: null },
      ],
    });
    assert.deepEqual(input, {
      type: 'agent_toolset_20260401',
      defaultConfig: { enabled: false, permissionPolicy: { type: 'always_ask' } },
      configs: [
        { name, type: name, enabled: true, permissionPolicy: { type: 'always_allow' } },
        { name, enabled: null, permissionPolicy: null },
      ],
    });
    const [resolved] = resolveAgentTools([input]);
    assert.deepEqual(resolved, {
      type: 'agent_toolset_20260401',
      defaultConfig: { enabled: false, permissionPolicy: { type: 'always_ask' } },
      configs: [
        { name, type: name, enabled: true, permissionPolicy: { type: 'always_allow' } },
        { name, type: name, enabled: false, permissionPolicy: { type: 'always_ask' } },
      ],
    });
    assert.deepEqual(fromAgentToolInput(resolved), {
      type: 'agent_toolset_20260401',
      default_config: { enabled: false, permission_policy: { type: 'always_ask' } },
      configs: [
        { name, type: name, enabled: true, permission_policy: { type: 'always_allow' } },
        { name, type: name, enabled: false, permission_policy: { type: 'always_ask' } },
      ],
    });
  });
}
