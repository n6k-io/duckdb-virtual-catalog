import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { ConnectionLike } from "../connection-shape";
import type { BackendHandle } from "./conftest/types";

// Aggregate pushdown: the optimizer rewrites Aggregate(Get(n6k_scan)) into a single OP_AGGREGATE
// request so the server groups and only the reduced result crosses the wire.
//
// Two things have to hold, and they are tested differently:
//   - Correctness. Every query is compared against `local_users`, a TEMP copy of the same rows.
//     A local table has no n6k scan, so it can never push -- it is the same DuckDB computing the
//     same aggregate by the untouched path. This catches wrong values AND wrong result types,
//     since snap() captures the Arrow schema too.
//   - That the rule fired (or declined). Results alone cannot show this: a rejected query still
//     returns the right answer via fallback. n6k_agg_pushdown_stats() supplies the delta.
//
// Value literals are avoided where a type is backend-dependent: sum(INTEGER) is HUGEINT on both
// backends, but marshals to JS as a BigInt on native and a decimal on browser-threads. Comparing
// against the local baseline sidesteps that; typeof() is asserted separately.

type Stats = {
  considered: number;
  pushed: number;
  rejected: number;
  reason: string;
};

async function readStats(conn: ConnectionLike): Promise<Stats> {
  const r = await conn.query(
    "SELECT considered, pushed, rejected, last_reject_reason FROM n6k_agg_pushdown_stats()",
  );
  const row = (
    r.toArray()[0] as { toJSON(): Record<string, unknown> }
  ).toJSON();
  return {
    considered: Number(row.considered),
    pushed: Number(row.pushed),
    rejected: Number(row.rejected),
    reason: String(row.last_reject_reason ?? ""),
  };
}

async function pushedDelta(
  conn: ConnectionLike,
  sql: string,
): Promise<{ pushed: number; rejected: number; reason: string }> {
  const before = await readStats(conn);
  await conn.query(sql);
  const after = await readStats(conn);
  return {
    pushed: after.pushed - before.pushed,
    rejected: after.rejected - before.rejected,
    reason: after.reason,
  };
}

const sumTypeSql = (from: string) =>
  `SELECT sum(id) AS s, typeof(sum(id)) AS t FROM ${from}`;

const allAggsSql = (from: string) =>
  `SELECT count(*) AS c0, count(name) AS c1, sum(id) AS s, min(name) AS mn, ` +
  `max(age) AS mx, avg(age) AS av FROM ${from}`;

const orFilterSql = (from: string) =>
  `SELECT count(*) AS c FROM ${from} WHERE age > 30 OR age < 26`;

const havingSql = (from: string) =>
  `SELECT age, count(*) AS c FROM ${from} GROUP BY age HAVING count(*) > 0 ORDER BY age`;

backendDescribe()(`aggregate pushdown [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    await handle.conn.query(`ATTACH '${N6K_URL}' AS slowdb (TYPE n6k)`);
    // A local copy is the differential baseline: no n6k scan, so no pushdown.
    await handle.conn.query(
      "CREATE OR REPLACE TEMP TABLE local_users AS SELECT * FROM db.main.users",
    );
  }, 30_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("server capability is advertised", async () => {
    const r = await snap(handle.conn, "SELECT * FROM n6k_capabilities('db')");
    expect(r.rows).toEqual([{ capability: "aggregate_pushdown" }]);
  });

  test("grouped aggregate pushes and matches the local baseline", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, count(*) AS c, sum(id) AS s FROM db.main.users GROUP BY age ORDER BY age",
    );
    const local = await snap(
      handle.conn,
      "SELECT age, count(*) AS c, sum(id) AS s FROM local_users GROUP BY age ORDER BY age",
    );
    expect(pushed).toEqual(local);
  });

  test("sum(INTEGER) reconciles back to HUGEINT", async () => {
    const pushed = await snap(handle.conn, sumTypeSql("db.main.users"));
    expect((pushed.rows[0] as Record<string, unknown>).t).toBe("HUGEINT");
    expect(pushed).toEqual(await snap(handle.conn, sumTypeSql("local_users")));
  });

  test("all whitelisted aggregates match the local baseline", async () => {
    const pushed = await snap(handle.conn, allAggsSql("db.main.users"));
    const local = await snap(handle.conn, allAggsSql("local_users"));
    expect(pushed).toEqual(local);
  });

  test("filtered aggregate pushes and matches the local baseline", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, sum(id) AS s FROM db.main.users WHERE age > 26 GROUP BY age ORDER BY age",
    );
    const local = await snap(
      handle.conn,
      "SELECT age, sum(id) AS s FROM local_users WHERE age > 26 GROUP BY age ORDER BY age",
    );
    expect(pushed).toEqual(local);
  });

  test("the rule actually fires", async () => {
    const r = await pushedDelta(
      handle.conn,
      "SELECT age, count(*) FROM db.main.users GROUP BY age",
    );
    expect(r.pushed).toBe(1);
    expect(r.rejected).toBe(0);
  });

  test("ungrouped aggregate over no rows is exactly one row", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT count(*) AS c, min(name) AS mn FROM db.main.users WHERE id < 0",
    );
    expect(pushed.rows).toEqual([{ c: "bigint:0", mn: null }]);
  });

  test("grouped aggregate over no rows is empty", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, count(*) AS c FROM db.main.users WHERE id < 0 GROUP BY age",
    );
    expect(pushed.rows).toEqual([]);
  });

  test("provider-backed table pushes and stays correct", async () => {
    const r = await pushedDelta(
      handle.conn,
      "SELECT count(*) FROM slowdb.main.slow",
    );
    expect(r.pushed).toBe(1);
    const got = await snap(
      handle.conn,
      "SELECT count(*) AS c, min(value) AS mn FROM slowdb.main.slow",
    );
    expect(got.rows).toEqual([{ c: "bigint:3", mn: "a" }]);
  });

  // Each must fall back with correct results — a rejection is never allowed to change an answer.
  const rejections: Array<[string, string, string]> = [
    [
      "expression group key",
      "lower(name)",
      "SELECT lower(name) AS g, count(*) AS c FROM $T GROUP BY lower(name) ORDER BY 1",
    ],
    [
      "DISTINCT aggregate",
      "DISTINCT",
      "SELECT count(DISTINCT age) AS c FROM $T",
    ],
    ["unsupported aggregate", "median", "SELECT median(age) AS m FROM $T"],
    [
      "string_agg",
      "string_agg",
      "SELECT string_agg(name, ',') AS s FROM (SELECT name FROM $T ORDER BY name)",
    ],
    ["expression argument", "expression", "SELECT sum(id + age) AS s FROM $T"],
    [
      "aggregate FILTER clause",
      "FILTER",
      "SELECT count(*) FILTER (WHERE age > 26) AS c FROM $T",
    ],
  ];

  for (const [label, , template] of rejections) {
    test(`falls back correctly: ${label}`, async () => {
      const remote = template.replace("$T", "db.main.users");
      const local = template.replace("$T", "local_users");
      const r = await pushedDelta(handle.conn, remote);
      expect(r.pushed).toBe(0);
      expect(await snap(handle.conn, remote)).toEqual(
        await snap(handle.conn, local),
      );
    });
  }

  test("OR-filtered aggregate does not push but stays correct", async () => {
    const r = await pushedDelta(handle.conn, orFilterSql("db.main.users"));
    expect(r.pushed).toBe(0);
    expect(await snap(handle.conn, orFilterSql("db.main.users"))).toEqual(
      await snap(handle.conn, orFilterSql("local_users")),
    );
  });

  test("HAVING pushes the aggregate and filters locally", async () => {
    const r = await pushedDelta(handle.conn, havingSql("db.main.users"));
    expect(r.pushed).toBe(1);
    expect(await snap(handle.conn, havingSql("db.main.users"))).toEqual(
      await snap(handle.conn, havingSql("local_users")),
    );
  });
});
