import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import { serverExec, pushInvalidate } from "./conftest/server";

const TIMEOUT = 30_000;

// App-supplied WebSockets are registered with the in-page driver, so sockets are
// created and asserted on in-page. PUSH interleaves node-side server calls.
describeCoi("registerWebsocket [browser coi]", () => {
  let h: BrowserHarness;
  let seq = 0;

  beforeAll(async () => {
    h = await setupCoiPage();
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  const name = (prefix: string): string => `${prefix}_${Date.now()}_${seq++}`;

  test(
    "attaches over a registered socket and resolves SELECT",
    async () => {
      const rows = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const id = t.registerWebsocket(ws);
        await new Promise<void>((res, rej) => {
          if (ws.readyState === WebSocket.OPEN) return res();
          ws.addEventListener("open", () => res(), { once: true });
          ws.addEventListener("error", () => rej(new Error("ws open")), {
            once: true,
          });
        });
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const r = await c.query(
            `SELECT id, name FROM ${cat}.main.users ORDER BY id`,
          );
          return r
            .toArray()
            .map((x) => [Number(x.toJSON().id), x.toJSON().name]);
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, name("wsreg"));
      expect(rows).toEqual([
        [1, "Alice"],
        [2, "Bob"],
        [3, "Charlie"],
      ]);
    },
    TIMEOUT,
  );

  test(
    "binary-split is non-destructive: the app keeps the socket",
    async () => {
      const res = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const id = t.registerWebsocket(ws);
        let appSawBinary = 0;
        ws.addEventListener("message", (ev) => {
          if (ev.data instanceof ArrayBuffer) appSawBinary++;
        });
        await new Promise<void>((res, rej) => {
          if (ws.readyState === WebSocket.OPEN) return res();
          ws.addEventListener("open", () => res(), { once: true });
          ws.addEventListener("error", () => rej(new Error("ws open")), {
            once: true,
          });
        });
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          await c.query(`SELECT count(*) FROM ${cat}.main.users`);
          return { onmessageNull: ws.onmessage === null, appSawBinary };
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, name("wssplit"));
      expect(res.onmessageNull).toBe(true);
      expect(res.appSawBinary).toBeGreaterThan(0);
    },
    TIMEOUT,
  );

  test(
    "replaceWebsocket rebinds a live catalog without DETACH/ATTACH",
    async () => {
      const counts = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const open = (ws: WebSocket) =>
          new Promise<void>((res, rej) => {
            if (ws.readyState === WebSocket.OPEN) return res();
            ws.addEventListener("open", () => res(), { once: true });
            ws.addEventListener("error", () => rej(new Error("ws open")), {
              once: true,
            });
          });
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const id = t.registerWebsocket(ws);
        await open(ws);
        const c = await t.connect();
        const count = async (): Promise<number> => {
          const r = await c.query(
            `SELECT count(*) AS n FROM ${cat}.main.users`,
          );
          return Number(r.toArray()[0]!.toJSON().n);
        };
        let ws2: WebSocket | undefined;
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const before = await count();
          ws.close();
          ws2 = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
          t.replaceWebsocket(id, ws2);
          await open(ws2);
          const after = await count();
          return { before, after };
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws2?.close();
        }
      }, name("wsrepl"));
      expect(counts.before).toBe(3);
      expect(counts.after).toBe(3);
    },
    TIMEOUT,
  );

  test(
    "PUSH on a registered socket reaches the catalog after invalidate",
    async () => {
      const catA = name("pushA");
      const view = "pushed_view";
      // Step 1 (in-page): open+register socket, attach, prime the catalog.
      await h.page.evaluate(
        async ({ cat, wsUrlCatalog }) => {
          const t = globalThis.__n6kTest!;
          const ws = new WebSocket(wsUrlCatalog);
          const id = t.registerWebsocket(ws);
          await new Promise<void>((res, rej) => {
            if (ws.readyState === WebSocket.OPEN) return res();
            ws.addEventListener("open", () => res(), { once: true });
            ws.addEventListener("error", () => rej(new Error("ws open")), {
              once: true,
            });
          });
          const c = await t.connect();
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          await c.query(`SELECT id FROM ${cat}.main.users`);
          const scratch = globalThis as unknown as {
            __pushC?: unknown;
            __pushWs?: WebSocket;
          };
          scratch.__pushC = c;
          scratch.__pushWs = ws;
        },
        { cat: catA, wsUrlCatalog: `${await wsUrl(h)}?catalog=${catA}` },
      );

      // Step 2 (node): create the view server-side; the local catalog is now stale.
      await serverExec(
        `CREATE VIEW ${catA}.main."${view}" AS SELECT 1 AS x`,
        catA,
      );
      const stale = await h.page.evaluate(
        async ({ cat, v }) => {
          const c = (
            globalThis as unknown as {
              __pushC: { query(s: string): Promise<unknown> };
            }
          ).__pushC;
          try {
            await c.query(`SELECT x FROM ${cat}.main."${v}"`);
            return false;
          } catch {
            return true;
          }
        },
        { cat: catA, v: view },
      );
      expect(stale).toBe(true);

      // Step 3 (node): invalidate; PUSH should refresh the local catalog.
      expect(await pushInvalidate(catA, "main")).toBe(1);

      const rows = await h.page.evaluate(
        async ({ cat, v }) => {
          const c = (
            globalThis as unknown as {
              __pushC: {
                query(s: string): Promise<{
                  toArray(): Array<{ toJSON(): Record<string, unknown> }>;
                }>;
              };
              __pushWs: WebSocket;
            }
          ).__pushC;
          let out: Record<string, unknown>[] = [];
          const deadline = performance.now() + 5000;
          while (performance.now() < deadline) {
            try {
              const res = await c.query(`SELECT x FROM ${cat}.main."${v}"`);
              out = res.toArray().map((r) => r.toJSON());
              if (out.length > 0) break;
            } catch {
              /* ignore */
            }
            await new Promise((r) => setTimeout(r, 50));
          }
          const s = globalThis as unknown as { __pushWs: WebSocket };
          await (c as unknown as { query(s: string): Promise<unknown> })
            .query(`DETACH ${cat}`)
            .catch(() => {});
          s.__pushWs.close();
          return out;
        },
        { cat: catA, v: view },
      );
      expect(rows).toEqual([{ x: 1 }]);
    },
    TIMEOUT,
  );
});

async function wsUrl(h: BrowserHarness): Promise<string> {
  return h.page.evaluate(() => globalThis.__n6kTest!.wsUrl);
}
