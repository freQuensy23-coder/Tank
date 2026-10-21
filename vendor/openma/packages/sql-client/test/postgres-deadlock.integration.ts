import { afterAll, beforeAll, describe, expect, it } from "vitest";
import postgres from "postgres";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { PostgresSqlClient } from "../src/adapters/postgres";
import {
  setDbBoundaryLogger,
  withDbBoundary,
  type DbBoundaryLog,
} from "../src/db-errors";

let container: StartedPostgreSqlContainer;
let url: string;

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:16-alpine").start();
  url = container.getConnectionUri();
  const admin = postgres(url, { max: 1 });
  try {
    await admin.unsafe(`
      CREATE TABLE deadlock_probe (
        id text PRIMARY KEY,
        revision integer NOT NULL
      );
      INSERT INTO deadlock_probe (id, revision) VALUES ('a', 0), ('b', 0);
    `);
  } finally {
    await admin.end();
  }
}, 180_000);

afterAll(async () => {
  await container?.stop();
}, 60_000);

describe("PostgreSQL deadlock retry", () => {
  it("classifies a real 40P01 as Deadlock and retries the batch successfully", async () => {
    const logs: DbBoundaryLog[] = [];
    setDbBoundaryLogger((record) => logs.push(record));
    let captured: DbBoundaryLog | undefined;
    for (let attempt = 0; attempt < 12 && captured === undefined; attempt += 1) {
      const raw = postgres(url, { max: 1 });
      const sql = postgres(url, { max: 1 });
      const client = new PostgresSqlClient(sql as never);
      const before = logs.length;
      let batch: Promise<unknown> | undefined;
      try {
        const holder = raw.begin(async (tx) => {
          await tx.unsafe("UPDATE deadlock_probe SET revision = revision + 1 WHERE id = 'b'");
          batch = withDbBoundary({
            op: "deadlock_probe",
            sessionId: "session_probe",
          }, () => client.batch([
            client.prepare("UPDATE deadlock_probe SET revision = revision + 1 WHERE id = ?").bind("a"),
            client.prepare("UPDATE deadlock_probe SET revision = revision + 1 WHERE id = ?").bind("b"),
          ]));
          await new Promise((resolve) => setTimeout(resolve, 200));
          await tx.unsafe("UPDATE deadlock_probe SET revision = revision + 1 WHERE id = 'a'").catch(() => undefined);
          throw new Error("rollback-holder");
        });
        await holder.catch(() => undefined);
        await batch?.catch(() => undefined);
        captured = logs.slice(before).find((entry) =>
          entry.errorTag === "Deadlock" && entry.outcome === "ok" && entry.retryCount > 0,
        );
      } finally {
        await raw.end().catch(() => undefined);
        await sql.end().catch(() => undefined);
      }
    }
    expect(captured).toMatchObject({
      op: "deadlock_probe",
      sessionId: "session_probe",
      outcome: "ok",
      errorTag: "Deadlock",
    });
    expect(captured?.retryCount ?? 0).toBeGreaterThan(0);
  }, 90_000);
});
