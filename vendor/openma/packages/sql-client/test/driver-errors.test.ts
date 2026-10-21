import { describe, expect, it } from "vitest";
import { ConnectionLost, Deadlock, LockWaitTimeout, driverRetry, isTransientDbError } from "../src/db-errors";
import { translateBetterSqlite3Error } from "../src/adapters/better-sqlite3-errors";
import { translateCfD1Error } from "../src/adapters/cf-d1-errors";
import { translateMysql2Error } from "../src/adapters/mysql2-errors";
import { translatePostgresError } from "../src/adapters/postgres-errors";

function coded(code: string, extra: Record<string, unknown> = {}): Error {
  return Object.assign(new Error(code), { code, ...extra });
}

describe("mysql2 translation", () => {
  it("maps 1213, 1205, and connection loss", () => {
    expect(translateMysql2Error(Object.assign(new Error("deadlock"), { errno: 1213, code: "ER_LOCK_DEADLOCK" }))).toBeInstanceOf(Deadlock);
    expect(translateMysql2Error(Object.assign(new Error("lock wait"), { errno: 1205, code: "ER_LOCK_WAIT_TIMEOUT" }))).toBeInstanceOf(LockWaitTimeout);
    expect(translateMysql2Error(coded("PROTOCOL_CONNECTION_LOST", { errno: 2013 }))).toBeInstanceOf(ConnectionLost);
  });

  it("reads a code nested on cause and ignores PostgreSQL SQLSTATEs", () => {
    const wrapped = new Error("pool");
    (wrapped as Error & { cause?: unknown }).cause = Object.assign(new Error("deadlock"), { errno: 1213 });
    expect(translateMysql2Error(wrapped)).toBeInstanceOf(Deadlock);
    const pg = coded("40P01");
    expect(translateMysql2Error(pg)).toBe(pg);
  });
});

describe("postgres translation", () => {
  it("maps 40P01 and serialization_failure 40001 to Deadlock", () => {
    expect(translatePostgresError(coded("40P01"))).toBeInstanceOf(Deadlock);
    expect(translatePostgresError(coded("40001"))).toBeInstanceOf(Deadlock);
  });

  it("maps 55P03 to LockWaitTimeout", () => {
    expect(translatePostgresError(coded("55P03"))).toBeInstanceOf(LockWaitTimeout);
  });

  it("maps shutdown, connection SQLSTATEs, postgres.js, and node-postgres disconnects", () => {
    for (const code of ["57P01", "57P02", "57P03", "08000", "08003", "08006", "08001", "08004", "CONNECTION_CLOSED"]) {
      expect(translatePostgresError(coded(code)), code).toBeInstanceOf(ConnectionLost);
    }
    expect(translatePostgresError(new Error("Connection terminated unexpectedly"))).toBeInstanceOf(ConnectionLost);
    expect(translatePostgresError(new Error("Connection terminated"))).toBeInstanceOf(ConnectionLost);
    expect(translatePostgresError(new Error("Client was closed and is not queryable"))).toBeInstanceOf(ConnectionLost);
  });

  it("reads a SQLSTATE nested on cause and leaves constraint errors alone", () => {
    const wrapped = new Error("query failed");
    (wrapped as Error & { cause?: unknown }).cause = coded("40P01");
    expect(translatePostgresError(wrapped)).toBeInstanceOf(Deadlock);
    const unique = coded("23505");
    expect(translatePostgresError(unique)).toBe(unique);
  });
});

describe("better-sqlite3 translation", () => {
  it("maps SQLITE_BUSY and SQLITE_LOCKED, including extended codes", () => {
    expect(translateBetterSqlite3Error(coded("SQLITE_BUSY"))).toBeInstanceOf(LockWaitTimeout);
    expect(translateBetterSqlite3Error(coded("SQLITE_BUSY_SNAPSHOT"))).toBeInstanceOf(LockWaitTimeout);
    expect(translateBetterSqlite3Error(coded("SQLITE_LOCKED"))).toBeInstanceOf(LockWaitTimeout);
    expect(translateBetterSqlite3Error(coded("SQLITE_LOCKED_SHAREDCACHE"))).toBeInstanceOf(LockWaitTimeout);
    expect(translateBetterSqlite3Error(Object.assign(new Error("busy"), { errcode: 517 }))).toBeInstanceOf(LockWaitTimeout);
    expect(translateBetterSqlite3Error(Object.assign(new Error("locked"), { errcode: 262 }))).toBeInstanceOf(LockWaitTimeout);
  });

  it("does not treat SQLITE_CONSTRAINT as transient", () => {
    const constraint = coded("SQLITE_CONSTRAINT");
    expect(translateBetterSqlite3Error(constraint)).toBe(constraint);
  });
});

describe("cf-d1 translation", () => {
  it("maps documented disconnect and reset text to ConnectionLost", () => {
    for (const message of [
      "D1_ERROR: Network connection lost.",
      "D1_ERROR: Replica disconnected from primary.",
      "D1_ERROR: Cannot resolve D1 DB due to transient issue on remote node.",
      "D1_ERROR: Can't read from request stream because client disconnected.",
      "D1_ERROR: D1 DB reset because its code was updated.",
      "D1_ERROR: Internal error while starting up D1 DB storage caused object to be reset.",
      "D1_ERROR: Internal error in D1 DB storage caused object to be reset.",
    ]) {
      expect(translateCfD1Error(new Error(message)), message).toBeInstanceOf(ConnectionLost);
    }
  });

  it("maps documented overload text to LockWaitTimeout and reads cause", () => {
    expect(translateCfD1Error(new Error("D1_ERROR: D1 DB is overloaded. Requests queued for too long."))).toBeInstanceOf(LockWaitTimeout);
    expect(translateCfD1Error(new Error("D1_ERROR: D1 DB is overloaded. Too many requests queued."))).toBeInstanceOf(LockWaitTimeout);
    const wrapped = new Error("D1_ERROR");
    (wrapped as Error & { cause?: unknown }).cause = new Error("Network connection lost.");
    expect(translateCfD1Error(wrapped)).toBeInstanceOf(ConnectionLost);
  });

  it("does not retry a syntax error", () => {
    const syntax = new Error("D1_EXEC_ERROR: near \"INSERTZ\": syntax error");
    expect(translateCfD1Error(syntax)).toBe(syntax);
  });
});

describe("shared retry wrapper", () => {
  it("retries the whole operation when the translator returns a transient tag", async () => {
    let calls = 0;
    const retry = driverRetry((error) =>
      translatePostgresError(error));
    const value = await retry(async () => {
      calls += 1;
      if (calls === 1) throw coded("40P01");
      return "ok";
    });
    expect(value).toBe("ok");
    expect(calls).toBe(2);
    expect(isTransientDbError(translatePostgresError(coded("40P01")))).toBe(true);
    expect(isTransientDbError(coded("40P01"))).toBe(false);
  });
});
