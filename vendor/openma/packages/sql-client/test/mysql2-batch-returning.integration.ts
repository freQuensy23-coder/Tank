import { describe, expect, it } from "vitest";
import { Mysql2SqlClient } from "../src/adapters/mysql2";

describe("Mysql2SqlClient transactional claim translation", () => {
  it("emulates UPDATE RETURNING inside the same batch transaction and removes same-table selectors", async () => {
    const executed: string[] = [];
    const connection = {
      beginTransaction: async () => { executed.push("BEGIN"); },
      commit: async () => { executed.push("COMMIT"); },
      rollback: async () => { executed.push("ROLLBACK"); },
      release: () => {},
      async execute(text: string) {
        executed.push(text);
        if (text.startsWith("SHOW KEYS")) return [[
          { Column_name: "workspace_id", Seq_in_index: 1 },
          { Column_name: "id", Seq_in_index: 2 },
        ], undefined] as const;
        if (text.startsWith("SELECT `workspace_id`, `id` FROM")) {
          return [[{ workspace_id: "ws", id: "execution" }], undefined] as const;
        }
        if (text.startsWith("SELECT id FROM")) return [[{ id: "execution" }], undefined] as const;
        return [{ affectedRows: 1 }, undefined] as const;
      },
    };
    const client = new Mysql2SqlClient({ getConnection: async () => connection } as never);
    const result = await client.batch<{ id: string }>([
      client.prepare(`INSERT INTO session_lock (workspace_id, session_id, claim_token)
        VALUES (?, ?, ?) ON CONFLICT (workspace_id, session_id) DO UPDATE SET claim_token = excluded.claim_token`)
        .bind("ws", "session", "token"),
      client.prepare(`UPDATE claims SET state = 'running' WHERE (workspace_id, id) = (
        SELECT candidate.workspace_id, candidate.id FROM claims AS candidate
        WHERE candidate.workspace_id = ? LIMIT 1
      ) AND EXISTS (SELECT 1 FROM session_lock AS lock
        WHERE lock.workspace_id = claims.workspace_id AND lock.claim_token = ?)
        RETURNING id`).bind("ws", "token"),
    ]);
    expect(result[1]).toMatchObject({ meta: { changes: 1 }, results: [{ id: "execution" }] });
    expect(executed).toContain("COMMIT");
    const mutation = executed.find((value) => value.startsWith("UPDATE `claims`"));
    expect(mutation).toContain("session_lock");
    expect(mutation).not.toContain("FROM claims AS candidate");
    expect(executed).not.toContain("ROLLBACK");
  });
});
