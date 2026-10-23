import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { TANK_ANALYST_AGENT, tankAnalystTools, withTankAnalystTools } from "./tank-tools.ts";

const toolset = { type: "agent_toolset_20260401", configs: [], default_config: { enabled: true } };

test("analyst sessions get the image catalog instead of their snapshot custom tools", () => {
  const stale = { type: "custom", name: "get_item", description: "old", input_schema: {} };
  const agent = withTankAnalystTools({ name: TANK_ANALYST_AGENT, tools: [toolset, stale] });
  const names = agent.tools.filter((tool) => tool.type === "custom").map((tool) => tool.name);
  assert.equal(agent.tools[0], toolset);
  assert.ok(names.includes("send_telegram_file") && names.includes("set_watch_condition"));
  assert.notEqual(agent.tools.find((tool) => tool.name === "get_item").description, "old");
  assert.equal(names.length, new Set(names).size);
});

test("other agents keep their own tools", () => {
  const scout = { name: "tank-scout-reviewer", tools: [toolset] };
  assert.equal(withTankAnalystTools(scout), scout);
});

test("the catalog is the analyst tools.json of the Tank configuration", () => {
  const config = JSON.parse(
    readFileSync(new URL("../../../../../../../config/skills/tank-event-analyst/tools.json", import.meta.url), "utf8"),
  );
  assert.deepEqual(
    tankAnalystTools().map(({ type, ...tool }) => tool),
    config,
  );
});
