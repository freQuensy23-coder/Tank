import { readFileSync } from "node:fs";

export const TANK_ANALYST_AGENT = "tank-event-analyst";

type CustomTool = { type: "custom"; name: string; description: string; input_schema: Record<string, unknown> };
type AgentTools<T> = { name?: string; tools: ReadonlyArray<T> };

let catalog: CustomTool[] | undefined;

/** The custom tools of Tank's analyst, built into this image beside this module. */
export function tankAnalystTools(): CustomTool[] {
  catalog ??= (
    JSON.parse(readFileSync(new URL("./tank-event-analyst.json", import.meta.url), "utf8")) as Array<
      Omit<CustomTool, "type">
    >
  ).map((tool) => ({ type: "custom", ...tool }));
  return catalog;
}

/** Every turn of a Tank analyst session uses the custom tools of the running image, so a
 * session created before a tool change gains the new tool without being re-registered. */
export function withTankAnalystTools<T extends { type?: string }, A extends AgentTools<T>>(agent: A): A {
  if (agent.name !== TANK_ANALYST_AGENT) return agent;
  return {
    ...agent,
    tools: [...agent.tools.filter((tool) => tool.type !== "custom"), ...(tankAnalystTools() as unknown as T[])],
  };
}
