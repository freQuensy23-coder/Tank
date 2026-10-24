import { pathToFileURL } from "node:url";
import { test } from "node:test";
import assert from "node:assert/strict";
import type { ReplayAttachmentContext, PrefixVerifier } from "../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts";
import type { Allocation } from "../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts";

const configured = process.env.TANK_AUDIT_OPENMA_ROOT;
const openmaSource = configured === undefined
  ? new URL("../../../vendor/openma/", import.meta.url)
  : pathToFileURL(configured.replace(/\/?$/, "/"));
const replay: typeof import("../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts") =
  await import(new URL("packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts", openmaSource).href);
const selected: Awaited<ReturnType<PrefixVerifier>> = { hostPath: "/owned/run_A", guestPath: "/replay/evidence", readOnly: true };
const allocation: Allocation = { image: "pinned", volumes: [selected] };
const context = (metadata: Record<string, string>): ReplayAttachmentContext => ({ environment: { name: "tank-replay" }, session: { metadata } });

test("latest mutable prefix is revalidated without mutating static allocation identity", async (): Promise<void> => {
  const calls: Parameters<PrefixVerifier>[0][] = [];
  const before = JSON.stringify(allocation);
  const verify = async (options: Parameters<PrefixVerifier>[0]): ReturnType<PrefixVerifier> => {
    calls.push(options);
    return selected;
  };
  for (const suffix of ["old", "new"]) await replay.validateReplayAttachment(context({
    tank_replay_run_id: "run_A", tank_replay_evidence_sha256: suffix, tank_replay_cutoff: suffix,
  }), allocation, verify, "/owned");
  assert.deepEqual(calls.map((call: Parameters<PrefixVerifier>[0]): string | undefined => call.manifestSha256), ["old", "new"]);
  assert.equal(JSON.stringify(allocation), before);
});

test("fresh validation failure and changed, writable or duplicate mounts fail closed", async (): Promise<void> => {
  await assert.rejects(replay.validateReplayAttachment(context({}), allocation, async (): ReturnType<PrefixVerifier> => {
    throw Error("current prefix mismatch");
  }, "/owned"), /current prefix mismatch/);
  for (const volumes of [[], [{ ...selected, hostPath: "/owned/run_B" }], [{ ...selected, readOnly: false }], [selected, selected]]) {
    await assert.rejects(replay.validateReplayAttachment(context({}), { ...allocation, volumes }, async (): ReturnType<PrefixVerifier> => selected, "/owned"), /retained_replay_mount_changed/);
  }
});

test("ordinary analysis attachments do not invoke replay validation", async (): Promise<void> => {
  await replay.validateReplayAttachment({ environment: { name: "tank-analysis" }, session: {} }, allocation, async (): ReturnType<PrefixVerifier> => assert.fail("no replay validation"), undefined);
});

