import {
  replayDatabaseRoute,
  sandboxDatabaseRoute,
  type ReplayDatabaseRoute,
  type SandboxDatabaseVariables,
  type SecretName,
} from "../vendor/openma/tank-deploy/openma_entrypoint.mjs";

interface SandboxProbeResult { defaults: string; isolated: string; rejected: number }
interface ReplayProbeResult { called: boolean; resolved: ReplayDatabaseRoute; denied: number }

function sandboxSecret(name: SecretName): string {
  if (name !== "tank_agent_db_password") throw Error("Sandbox route requested the wrong credential");
  return "synthetic:p@ss";
}

function rejectedIdentifier(input: SandboxDatabaseVariables): boolean {
  try {
    sandboxDatabaseRoute(input, sandboxSecret);
    return false;
  } catch (error: unknown) {
    if (!(error instanceof Error) || !error.message.includes("SQL identifier")) throw error;
    return true;
  }
}

function sandboxProbe(): SandboxProbeResult {
  const defaults = sandboxDatabaseRoute({}, sandboxSecret);
  const isolated = sandboxDatabaseRoute({
    TANK_SANDBOX_DATABASE_NAME: "tank_integration_extract_20261018",
    TANK_SANDBOX_DATABASE_ROLE: "tank_integration_extract_agent",
  }, sandboxSecret);
  let rejected = 0;
  for (const value of ["", "tank;DROP", "../tank", "A", "x".repeat(64)]) {
    if (rejectedIdentifier({ TANK_SANDBOX_DATABASE_NAME: value })) rejected++;
    if (rejectedIdentifier({ TANK_SANDBOX_DATABASE_ROLE: value })) rejected++;
  }
  return { defaults, isolated, rejected };
}

async function replayProbe(): Promise<ReplayProbeResult> {
  let called = false;
  const secret = (_name: SecretName): string => "synthetic";
  const resolved = await replayDatabaseRoute(secret, async (host: "postgres", options: { family: 4 }): Promise<{ address: string }> => {
    if (host !== "postgres" || options.family !== 4) throw Error("Replay route resolved the wrong service");
    called = true;
    return { address: "172.30.42.2" };
  });
  let denied = 0;
  for (const address of ["8.8.8.8", "0.0.0.0", "127.0.0.1", "::1"]) {
    try {
      await replayDatabaseRoute(secret, async (): Promise<{ address: string }> => ({ address }));
    } catch (error: unknown) {
      if (!(error instanceof Error) || !error.message.includes("private IPv4 service address")) throw error;
      denied++;
    }
  }
  return { called, resolved, denied };
}

const mode = process.argv[2];
if (mode === "sandbox") console.log(JSON.stringify(sandboxProbe()));
else if (mode === "replay") console.log(JSON.stringify(await replayProbe()));
else throw Error("A declared sandbox or replay probe is required");
