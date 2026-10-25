import { readFile } from "node:fs/promises";
import { join } from "node:path";

export interface ProcessEvidence {
  pid: number;
  state: string;
  startTime: string;
  rssKiB: number | null;
  rssUnavailableReason: "KERNEL_OMITTED_VMRSS_FOR_TERMINATED_PROCESS" | null;
}

export function terminatedProcess(state: string): boolean {
  return state === "Z" || state === "X" || state === "x";
}

export async function processEvidence(pid: number, root: string): Promise<ProcessEvidence | null> {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw Error("Native process PID must be a positive safe integer");
  let stat: string;
  let status: string;
  try {
    stat = await readFile(join(root, String(pid), "stat"), "utf8");
    status = await readFile(join(root, String(pid), "status"), "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && (error.code === "ENOENT" || error.code === "ESRCH")) return null;
    throw error;
  }
  const closing = stat.lastIndexOf(")");
  if (closing < 0) throw Error("Native process stat has no command boundary");
  const fields = stat.slice(closing + 1).trim().split(/\s+/);
  const state = fields[0];
  const startTime = fields[19];
  if (state === undefined || !/^[A-Za-z]$/.test(state) || startTime === undefined || !/^\d+$/.test(startTime)) {
    throw Error("Native process stat lacks explicit state or start identity");
  }
  const resident = status.match(/^VmRSS:\s+(\d+)\s+kB$/m)?.[1];
  if (resident === undefined) {
    if (!terminatedProcess(state)) throw Error("Live native process RSS evidence is unavailable");
    return { pid, state, startTime, rssKiB: null, rssUnavailableReason: "KERNEL_OMITTED_VMRSS_FOR_TERMINATED_PROCESS" };
  }
  const rssKiB = Number(resident);
  if (!Number.isSafeInteger(rssKiB) || rssKiB < 0) throw Error("Native process RSS evidence is outside the exact integer range");
  return { pid, state, startTime, rssKiB, rssUnavailableReason: null };
}
