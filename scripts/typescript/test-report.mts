export interface TestSummary {
  cases: number;
  passed: number;
  failed: number;
  cancelled: number;
  skipped: number;
  todo: number;
}

function counter(output: string, key: string): number {
  const matches = [...output.matchAll(new RegExp("^# " + key + " ([0-9]+)$", "gm"))];
  if (matches.length !== 1 || matches[0]?.[1] === undefined) throw Error("Missing unique Node test counter: " + key);
  const value = Number(matches[0][1]);
  if (!Number.isSafeInteger(value)) throw Error("Invalid Node test counter: " + key);
  return value;
}

export function verifyTestReport(output: string): TestSummary {
  const summary = {
    cases: counter(output, "tests"),
    passed: counter(output, "pass"),
    failed: counter(output, "fail"),
    cancelled: counter(output, "cancelled"),
    skipped: counter(output, "skipped"),
    todo: counter(output, "todo"),
  };
  if (
    summary.cases === 0 ||
    summary.passed !== summary.cases ||
    summary.failed !== 0 ||
    summary.cancelled !== 0 ||
    summary.skipped !== 0 ||
    summary.todo !== 0
  ) throw Error("Node tests require a complete nonempty set with zero failures, skips, cancellations or todos");
  return summary;
}
