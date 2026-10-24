import assert from "node:assert/strict";
import { test } from "node:test";
import { verifyTestReport } from "./test-report.mts";

const complete = "# tests 2\n# pass 2\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n";

test("Node gate rejects an incomplete execution report", (): void => {
  assert.throws((): void => {
    verifyTestReport(complete.replace("# pass 2", "# pass 1"));
  }, /complete nonempty/);
});

test("Node gate rejects skipped test evidence", (): void => {
  assert.throws((): void => {
    verifyTestReport(complete.replace("# skipped 0", "# skipped 1"));
  }, /zero failures, skips/);
});

test("Node gate rejects duplicate or absent summary counters", (): void => {
  for (const output of [complete + "# tests 2\n", complete.replace("# tests 2\n", "")]) {
    assert.throws((): void => {
      verifyTestReport(output);
    }, /Missing unique/);
  }
});
