import { test, expect, afterAll, beforeAll, describe } from "bun:test";
import {
  SAB_SIZE,
  FLAG,
  URL_LEN,
  HTTP_STATUS,
  CHUNK_OFFSET,
  CHUNK_LEN,
  BODY_LEN,
  REQUEST,
  RESPONSE_READY,
  IDLE,
  CHUNK_REQUEST,
  BODY_CHUNK_READY,
  BODY_CHUNK_REQUEST,
  DATA_OFFSET,
  CATALOG_REGION_SIZE,
} from "../protocol.js";

var dataCapacity = SAB_SIZE - DATA_OFFSET - CATALOG_REGION_SIZE;

function createWorkerEnv() {
  var sab = new SharedArrayBuffer(SAB_SIZE);
  var control = new Int32Array(sab, 0, 8);
  var urlBytes = new Uint8Array(sab, 32, 4096);
  var dataBytes = new Uint8Array(sab, DATA_OFFSET, dataCapacity);
  var worker = new Worker(
    new URL("../workers/fetch-worker.ts", import.meta.url),
  );
  worker.postMessage({ type: "init", sab });
  return { sab, control, urlBytes, dataBytes, worker };
}

async function waitFlag(control, index, oldVal) {
  await Atomics.waitAsync(control, index, oldVal).value;
}

function readText(dataBytes, len) {
  return new TextDecoder().decode(dataBytes.slice(0, len));
}

describe("fetch worker — DELETE", () => {
  var server, env, receivedRequests;

  beforeAll(() => {
    receivedRequests = [];
    server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        receivedRequests.push({ method: req.method });
        return new Response("deleted", { status: 200 });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("BODY_LEN = -1 sends DELETE request", async () => {
    var enc = new TextEncoder();
    var urlEncoded = enc.encode(`http://localhost:${server.port}/resource`);
    env.urlBytes.set(urlEncoded);
    Atomics.store(env.control, URL_LEN, urlEncoded.length);
    Atomics.store(env.control, BODY_LEN, -1);
    Atomics.store(env.control, FLAG, REQUEST);
    Atomics.notify(env.control, FLAG);

    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

    Atomics.store(env.control, CHUNK_OFFSET, 0);
    Atomics.store(env.control, FLAG, CHUNK_REQUEST);
    Atomics.notify(env.control, FLAG);
    await waitFlag(env.control, FLAG, CHUNK_REQUEST);

    var chunkLen = Atomics.load(env.control, CHUNK_LEN);
    expect(readText(env.dataBytes, chunkLen)).toBe("deleted");

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedRequests.length).toBe(1);
    expect(receivedRequests[0].method).toBe("DELETE");
  });
});

describe("fetch worker — multipart POST", () => {
  var server, env, receivedRequests;

  beforeAll(() => {
    receivedRequests = [];
    server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        receivedRequests.push({
          method: req.method,
          contentType: req.headers.get("content-type"),
          bodyBytes: new Uint8Array(await req.arrayBuffer()),
        });
        return new Response("ok", { status: 200 });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("sends binary multipart body via chunked upload", async () => {
    var enc = new TextEncoder();
    var boundary = "n6k";
    var bodyContent =
      '--n6k\r\nContent-Type: application/json\r\n\r\n{"key":"val"}\r\n--n6k--\r\n';
    var bodyBytes = enc.encode(bodyContent);

    var urlEncoded = enc.encode(`http://localhost:${server.port}/rpc/test`);
    env.urlBytes.set(urlEncoded);
    Atomics.store(env.control, URL_LEN, urlEncoded.length);

    Atomics.store(env.control, BODY_LEN, -bodyBytes.length);

    var ctHeader = enc.encode("multipart/mixed; boundary=" + boundary);
    env.dataBytes.set(ctHeader);
    Atomics.store(env.control, CHUNK_LEN, ctHeader.length);

    Atomics.store(env.control, FLAG, REQUEST);
    Atomics.notify(env.control, FLAG);

    await waitFlag(env.control, FLAG, REQUEST);
    expect(Atomics.load(env.control, FLAG)).toBe(BODY_CHUNK_REQUEST);

    env.dataBytes.set(bodyBytes);
    Atomics.store(env.control, CHUNK_LEN, bodyBytes.length);
    Atomics.store(env.control, CHUNK_OFFSET, 1);
    Atomics.store(env.control, FLAG, BODY_CHUNK_READY);
    Atomics.notify(env.control, FLAG);

    await waitFlag(env.control, FLAG, BODY_CHUNK_READY);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedRequests.length).toBe(1);
    expect(receivedRequests[0].method).toBe("POST");
    expect(receivedRequests[0].contentType).toBe(
      "multipart/mixed; boundary=n6k",
    );

    var received = new TextDecoder().decode(receivedRequests[0].bodyBytes);
    expect(received).toBe(bodyContent);
  });

  test("large binary body sent in multiple chunks", async () => {
    var enc = new TextEncoder();
    var boundary = "n6k";

    var padding = "X".repeat(dataCapacity + 500);
    var bodyContent =
      "--n6k\r\nContent-Type: application/json\r\n\r\n" +
      padding +
      "\r\n--n6k--\r\n";
    var bodyBytes = enc.encode(bodyContent);

    var urlEncoded = enc.encode(`http://localhost:${server.port}/rpc/large`);
    env.urlBytes.set(urlEncoded);
    Atomics.store(env.control, URL_LEN, urlEncoded.length);
    Atomics.store(env.control, BODY_LEN, -bodyBytes.length);

    var ctHeader = enc.encode("multipart/mixed; boundary=" + boundary);
    env.dataBytes.set(ctHeader);
    Atomics.store(env.control, CHUNK_LEN, ctHeader.length);

    Atomics.store(env.control, FLAG, REQUEST);
    Atomics.notify(env.control, FLAG);

    var offset = 0;
    while (offset < bodyBytes.length) {
      await waitFlag(env.control, FLAG, REQUEST);
      if (Atomics.load(env.control, FLAG) !== BODY_CHUNK_REQUEST) {
        await waitFlag(env.control, FLAG, BODY_CHUNK_READY);
        if (Atomics.load(env.control, FLAG) !== BODY_CHUNK_REQUEST) break;
      }

      var chunkSize = Math.min(bodyBytes.length - offset, dataCapacity);
      env.dataBytes.set(bodyBytes.subarray(offset, offset + chunkSize));
      offset += chunkSize;
      Atomics.store(env.control, CHUNK_LEN, chunkSize);
      Atomics.store(
        env.control,
        CHUNK_OFFSET,
        offset >= bodyBytes.length ? 1 : 0,
      );
      Atomics.store(env.control, FLAG, BODY_CHUNK_READY);
      Atomics.notify(env.control, FLAG);
    }

    await waitFlag(env.control, FLAG, BODY_CHUNK_READY);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    var lastReq = receivedRequests.at(-1);
    expect(lastReq.bodyBytes.length).toBe(bodyBytes.length);
  });
});
