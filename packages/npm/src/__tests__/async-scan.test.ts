import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { ConnectionLike } from "../connection-shape";
import type { BackendHandle } from "./conftest/types";

// The async scan source operator. The OptimizerExtension rewrites an `n6k_scan` into the BLOCKED
// source that yields its worker between frames, carrying any projection and any exactly-renderable
// filter onto the wire. Both backends take this path, so correctness and the `parks` counter must
// hold on both. Projection order matters: the operator decodes batches positionally but looks
// conversion data up by base column id, so a reordered projection is the case that catches a
// keying mistake.

async function readParks(conn: ConnectionLike): Promise<number> {
  const r = await conn.query("SELECT parks FROM n6k_async_scan_stats()");
  const row = r.toArray()[0] as { toJSON(): Record<string, unknown> };
  return Number(row.toJSON().parks);
}

backendDescribe()(`async scan [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    // slowdb.main.slow routes to the server's SlowProvider (~500ms scan) → a guaranteed park on native.
    await handle.conn.query(`ATTACH '${N6K_URL}' AS slowdb (TYPE n6k)`);
  }, 30_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  // A full SELECT * is the async path on native; the multi-type row set exercises the Arrow→DuckDB
  // conversion the operator drives manually (int + varchar).
  test("full scan returns correct rows (multi-type)", async () => {
    const got = await snap(
      handle.conn,
      "SELECT * FROM db.main.users ORDER BY id",
    );
    expect(got.rows).toEqual([
      { id: 1, name: "Alice", age: 30 },
      { id: 2, name: "Bob", age: 25 },
      { id: 3, name: "Charlie", age: 35 },
    ]);
  });

  // A subset in a DIFFERENT order than the table declares: `columns=` must be sent in request order
  // and each decoded batch column mapped back to its base id, or the values land in the wrong column.
  test("reordered projection returns correct columns", async () => {
    const got = await snap(
      handle.conn,
      "SELECT name, id FROM db.main.users ORDER BY id",
    );
    expect(got.rows).toEqual([
      { name: "Alice", id: 1 },
      { name: "Bob", id: 2 },
      { name: "Charlie", id: 3 },
    ]);
  });

  // Pushed filters are authoritative — nothing re-applies them locally — so a keying mistake here
  // returns wrong rows rather than an error.
  test("pushed filter returns correct rows", async () => {
    const got = await snap(
      handle.conn,
      "SELECT id, name FROM db.main.users WHERE age > 26 ORDER BY id",
    );
    expect(got.rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 3, name: "Charlie" },
    ]);
  });

  test("projection + filter together", async () => {
    const got = await snap(
      handle.conn,
      "SELECT name FROM db.main.users WHERE id = 2",
    );
    expect(got.rows).toEqual([{ name: "Bob" }]);
  });

  test("a slow full scan parks the worker at least once", async () => {
    const before = await readParks(handle.conn);
    await handle.conn.query("SELECT * FROM slowdb.main.slow");
    const after = await readParks(handle.conn);
    expect(after - before).toBeGreaterThanOrEqual(1);
  }, 15_000);

  // The regression this guards: a projected scan silently declining the rewrite and falling back to
  // the blocking stream. `parks` only moves on the async path.
  test("a slow projected scan parks the worker at least once", async () => {
    const before = await readParks(handle.conn);
    await handle.conn.query("SELECT value FROM slowdb.main.slow");
    const after = await readParks(handle.conn);
    expect(after - before).toBeGreaterThanOrEqual(1);
  }, 15_000);
});
