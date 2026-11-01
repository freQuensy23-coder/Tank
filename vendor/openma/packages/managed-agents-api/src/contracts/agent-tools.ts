import type {
  AgentCreateParams,
  BetaManagedAgentsAgentToolset20260401,
} from "@anthropic-ai/sdk/resources/beta/agents/agents";

type OfficialAgentToolInput = NonNullable<AgentCreateParams["tools"]>[number];
type OfficialAgentToolsetInput = Extract<
  OfficialAgentToolInput,
  { type: "agent_toolset_20260401" }
>;
type OfficialToolConfigInput = NonNullable<OfficialAgentToolsetInput["configs"]>[number];
type LocalToolName = "browser" | "schedule" | "cancel_schedule" | "list_schedules";
type LocalToolConfigInput = {
  [Name in LocalToolName]: Omit<
    Extract<OfficialToolConfigInput, { name: "bash" }>,
    "name" | "type"
  > & { name: Name; type?: Name };
}[LocalToolName];
type LocalToolConfig = {
  [Name in LocalToolName]: Omit<
    Extract<BetaManagedAgentsAgentToolset20260401["configs"][number], { name: "bash" }>,
    "name" | "type"
  > & { name: Name; type: Name };
}[LocalToolName];

export type OpenMaAgentToolInput =
  | Exclude<OfficialAgentToolInput, { type: "agent_toolset_20260401" }>
  | (Omit<OfficialAgentToolsetInput, "configs"> & {
      configs?: (OfficialToolConfigInput | LocalToolConfigInput)[];
    });

export type OpenMaAgentToolset = Omit<BetaManagedAgentsAgentToolset20260401, "configs"> & {
  configs: (BetaManagedAgentsAgentToolset20260401["configs"][number] | LocalToolConfig)[];
};
