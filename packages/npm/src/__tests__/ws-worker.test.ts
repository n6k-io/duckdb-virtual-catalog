import { test, expect, beforeAll } from "bun:test";
import {
  describeServer,
  SKIP_SERVER_TESTS,
  SERVER,
  WS_URL,
} from "./_server-gate";
import path from "node:path";
import { FT, OP, N6K_PROTOCOL_VERSION, type FrameHeader } from "../ws-framing";
import { WS_SAB_SIZE } from "../ws-sab";

const WORKER_URL = path.resolve(import.meta.dir, "../workers/ws-worker.ts");

async function serverUp(): Promise<boolean> {
  try {
    const r = await fetch(`${SERVER}/debug/counts`, {
      signal: AbortSignal.timeout(2000),
    });
    return r.ok;
  } catch {
    return false;
  }
}

beforeAll(async () => {
  if (SKIP_SERVER_TESTS) return;
  if (!(await serverUp())) {
    throw new Error(`n6k test server not running on ${SERVER}`);
  }
});

type ReadyMsg = {
  type: "ready";
  protocolVersion: number;
  maxConcurrentReqs: number;
  defaultBatchCredits: number;
};

type FrameMsg = {
  type: "frame";
  reqId: number;
  frame: { header: FrameHeader; body: Uint8Array };
  end: boolean;
};

type ClosedMsg = { type: "closed"; reason: string };
type ErrorMsg = { type: "error"; message: string };
type OutMsg = ReadyMsg | FrameMsg | ClosedMsg | ErrorMsg;

function openWorker(): { worker: Worker; next: () => Promise<OutMsg> } {
  const worker = new Worker(WORKER_URL);
  const queue: OutMsg[] = [];
  const waiters: ((m: OutMsg) => void)[] = [];
  worker.addEventListener("message", function (ev: MessageEvent) {
    const m = ev.data as OutMsg;
    const w = waiters.shift();
    if (w) w(m);
    else queue.push(m);
  });
  function next(): Promise<OutMsg> {
    return new Promise((resolve, reject) => {
      const t = setTimeout(
        () => reject(new Error("ws-worker message timeout")),
        5000,
      );
      const buffered = queue.shift();
      if (buffered) {
        clearTimeout(t);
        resolve(buffered);
      } else {
        waiters.push(function (m) {
          clearTimeout(t);
          resolve(m);
        });
      }
    });
  }
  return { worker, next };
}

describeServer("ws-worker v2", () => {
  test("HELLO_ACK round-trip", async () => {
    const { worker, next } = openWorker();
    try {
      worker.postMessage({
        type: "open",
        url: `${WS_URL}?catalog=db`,
      });
      const ready = (await next()) as ReadyMsg;
      expect(ready.type).toBe("ready");
      expect(ready.protocolVersion).toBe(N6K_PROTOCOL_VERSION);
      expect(ready.defaultBatchCredits).toBeGreaterThan(0);
    } finally {
      worker.terminate();
    }
  });

  test("bare WS dial without ?catalog= is refused", async () => {
    // The catalog used to be readable from the FT_HELLO frame. It is not any more: the
    // host in front of the server no longer parses frames, so it has to ATTACH the
    // catalog before handing the socket over and needs the name from the URL. Every
    // other dial in this file passes `?catalog=`, and both real clients already do.
    const { worker, next } = openWorker();
    try {
      const sab = new SharedArrayBuffer(WS_SAB_SIZE);
      worker.postMessage({ type: "init-sab", sab, catalog: "db" });
      worker.postMessage({ type: "open", url: WS_URL });
      const msg = (await next()) as ClosedMsg;
      expect(msg.type).toBe("closed");
    } finally {
      worker.terminate();
    }
  });

  test("CATALOG_LIST REQ/RESP", async () => {
    const { worker, next } = openWorker();
    try {
      worker.postMessage({
        type: "open",
        url: `${WS_URL}?catalog=db`,
      });
      const ready = (await next()) as ReadyMsg;
      expect(ready.type).toBe("ready");

      worker.postMessage({
        type: "req",
        reqId: 1,
        op: OP.CATALOG_LIST,
        body: new Uint8Array(),
      });

      let schemas: string[] | null = null;
      let sawEnd = false;
      for (let i = 0; i < 10 && !sawEnd; i++) {
        const m = (await next()) as FrameMsg;
        expect(m.type).toBe("frame");
        expect(m.reqId).toBe(1);
        switch (m.frame.header.t) {
          case FT.RESP_CHUNK: {
            schemas = m.frame.header.schemas as string[];

            break;
          }
          case FT.RESP_END: {
            sawEnd = true;
            expect(m.end).toBe(true);

            break;
          }
          case FT.ERR: {
            throw new Error(
              `unexpected ERR: ${JSON.stringify(m.frame.header)}`,
            );
          }
          // No default
        }
      }
      expect(sawEnd).toBe(true);
      expect(schemas).not.toBeNull();
      expect(schemas).toContain("main");
    } finally {
      worker.terminate();
    }
  });

  test("clean terminate does not throw", async () => {
    const { worker, next } = openWorker();
    worker.postMessage({
      type: "open",
      url: `${WS_URL}?catalog=db`,
    });
    const ready = (await next()) as ReadyMsg;
    expect(ready.type).toBe("ready");
    worker.terminate();
  });

  test("PUSH frames are tagged with the init-sab catalog", async () => {
    const { worker, next } = openWorker();
    try {
      const sab = new SharedArrayBuffer(WS_SAB_SIZE);
      worker.postMessage({ type: "init-sab", sab, catalog: "db" });
      worker.postMessage({
        type: "open",
        url: `${WS_URL}?catalog=db`,
      });
      const ready = (await next()) as ReadyMsg;
      expect(ready.type).toBe("ready");

      const r = await fetch(
        `${SERVER}/debug/push_invalidate?catalog=db&schemas=main`,
        { method: "POST" },
      );
      expect(r.ok).toBe(true);

      const push = (await next()) as unknown as {
        type: string;
        catalog: string;
        op: number;
        body: { schemas: string[] };
      };
      expect(push.type).toBe("push");
      expect(push.catalog).toBe("db");
      expect(push.op).toBe(OP.CATALOG_INVALIDATED);
      expect(push.body).toEqual({ schemas: ["main"] });
    } finally {
      worker.terminate();
    }
  });
});
