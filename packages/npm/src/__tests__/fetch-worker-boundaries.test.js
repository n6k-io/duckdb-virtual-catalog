import { test, expect, afterAll, beforeAll, describe } from "bun:test";
import {
  SAB_SIZE,
  FLAG,
  URL_LEN,
  RESP_LEN,
  HTTP_STATUS,
  CHUNK_OFFSET,
  CHUNK_LEN,
  BODY_LEN,
  REQUEST,
  RESPONSE_READY,
  IDLE,
  CHUNK_REQUEST,
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

function sendRequest(control, urlBytes, url) {
  var enc = new TextEncoder();
  var encoded = enc.encode(url);
  urlBytes.set(encoded);
  Atomics.store(control, URL_LEN, encoded.length);
  Atomics.store(control, BODY_LEN, 0);
  Atomics.store(control, FLAG, REQUEST);
  Atomics.notify(control, FLAG);
}

function readAllChunks(control, dataBytes, totalLen) {
  var chunks = [];
  var offset = 0;

  async function next() {
    while (offset < totalLen) {
      Atomics.store(control, CHUNK_OFFSET, offset);
      Atomics.store(control, FLAG, CHUNK_REQUEST);
      Atomics.notify(control, FLAG);
      await waitFlag(control, FLAG, CHUNK_REQUEST);

      var chunkLen = Atomics.load(control, CHUNK_LEN);
      chunks.push(new Uint8Array(dataBytes.slice(0, chunkLen)));
      offset += chunkLen;
    }
  }

  return next().then(function () {
    var total = new Uint8Array(totalLen);
    var pos = 0;
    for (var c of chunks) {
      total.set(c, pos);
      pos += c.length;
    }
    return { bytes: total, chunkCount: chunks.length };
  });
}

describe("fetch worker — chunk boundaries", () => {
  var server, env;

  beforeAll(() => {
    server = Bun.serve({
      port: 0,
      fetch: (req) => {
        var url = new URL(req.url);
        var size = Number.parseInt(url.searchParams.get("size"), 10);
        var buf = new Uint8Array(size);
        for (var i = 0; i < size; i++) {
          buf[i] = i % 256;
        }
        return new Response(buf, {
          status: 200,
          headers: { "Content-Type": "application/octet-stream" },
        });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("response exactly equal to dataCapacity fits in one chunk", async () => {
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/?size=${dataCapacity}`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    var totalLen = Atomics.load(env.control, RESP_LEN);
    expect(totalLen).toBe(dataCapacity);

    var { bytes, chunkCount } = await readAllChunks(
      env.control,
      env.dataBytes,
      totalLen,
    );
    expect(chunkCount).toBe(1);

    for (var i = 0; i < dataCapacity; i++) {
      if (bytes[i] !== i % 256) {
        throw new Error(
          `Byte mismatch at offset ${i}: got ${bytes[i]}, expected ${i % 256}`,
        );
      }
    }

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
    await new Promise((r) => setTimeout(r, 10));
  });

  test("response at dataCapacity + 1 requires exactly two chunks", async () => {
    var size = dataCapacity + 1;
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/?size=${size}`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    var totalLen = Atomics.load(env.control, RESP_LEN);
    expect(totalLen).toBe(size);

    var { bytes, chunkCount } = await readAllChunks(
      env.control,
      env.dataBytes,
      totalLen,
    );
    expect(chunkCount).toBe(2);
    expect(bytes.length).toBe(size);

    for (var i = 0; i < size; i++) {
      if (bytes[i] !== i % 256) {
        throw new Error(
          `Byte mismatch at offset ${i}: got ${bytes[i]}, expected ${i % 256}`,
        );
      }
    }

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
    await new Promise((r) => setTimeout(r, 10));
  });

  test("response at exactly 2x dataCapacity requires exactly two chunks", async () => {
    var size = dataCapacity * 2;
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/?size=${size}`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    var totalLen = Atomics.load(env.control, RESP_LEN);
    expect(totalLen).toBe(size);

    var { bytes, chunkCount } = await readAllChunks(
      env.control,
      env.dataBytes,
      totalLen,
    );
    expect(chunkCount).toBe(2);
    expect(bytes.length).toBe(size);

    for (var i = 0; i < size; i++) {
      if (bytes[i] !== i % 256) {
        throw new Error(
          `Byte mismatch at offset ${i}: got ${bytes[i]}, expected ${i % 256}`,
        );
      }
    }

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
    await new Promise((r) => setTimeout(r, 10));
  });

  test("response at 2x dataCapacity + 1 requires exactly three chunks", async () => {
    var size = dataCapacity * 2 + 1;
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/?size=${size}`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    var totalLen = Atomics.load(env.control, RESP_LEN);
    expect(totalLen).toBe(size);

    var { bytes, chunkCount } = await readAllChunks(
      env.control,
      env.dataBytes,
      totalLen,
    );
    expect(chunkCount).toBe(3);
    expect(bytes.length).toBe(size);

    for (var i = 0; i < size; i++) {
      if (bytes[i] !== i % 256) {
        throw new Error(
          `Byte mismatch at offset ${i}: got ${bytes[i]}, expected ${i % 256}`,
        );
      }
    }

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });
});

describe("fetch worker — sequential request isolation", () => {
  var server, env;

  beforeAll(() => {
    var requestCount = 0;
    server = Bun.serve({
      port: 0,
      fetch: () => {
        requestCount++;
        return new Response(`response-${requestCount}`, { status: 200 });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("three back-to-back requests each return correct data", async () => {
    var decoder = new TextDecoder();

    for (var n = 1; n <= 3; n++) {
      await waitFlag(
        env.control,
        FLAG,
        Atomics.load(env.control, FLAG) === IDLE
          ? -999
          : Atomics.load(env.control, FLAG),
      );

      sendRequest(
        env.control,
        env.urlBytes,
        `http://localhost:${server.port}/`,
      );
      await waitFlag(env.control, FLAG, REQUEST);

      expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
      expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

      Atomics.store(env.control, CHUNK_OFFSET, 0);
      Atomics.store(env.control, FLAG, CHUNK_REQUEST);
      Atomics.notify(env.control, FLAG);
      await waitFlag(env.control, FLAG, CHUNK_REQUEST);

      var chunkLen = Atomics.load(env.control, CHUNK_LEN);
      var text = decoder.decode(env.dataBytes.slice(0, chunkLen));
      expect(text).toBe(`response-${n}`);

      Atomics.store(env.control, FLAG, IDLE);
      Atomics.notify(env.control, FLAG);

      await new Promise((r) => setTimeout(r, 10));
    }
  }, 15_000);
});

function sendWithBody(control, urlBytes, url, body) {
  var enc = new TextEncoder();
  var urlEncoded = enc.encode(url);
  urlBytes.set(urlEncoded);
  Atomics.store(control, URL_LEN, urlEncoded.length);
  var bodyEncoded = enc.encode(body);
  urlBytes.set(bodyEncoded, urlEncoded.length);
  Atomics.store(control, BODY_LEN, bodyEncoded.length);
  Atomics.store(control, FLAG, REQUEST);
  Atomics.notify(control, FLAG);
}

async function drainRequest(control) {
  await waitFlag(control, FLAG, REQUEST);
  Atomics.store(control, FLAG, IDLE);
  Atomics.notify(control, FLAG);
  await new Promise((r) => setTimeout(r, 10));
}

describe("fetch worker — content-type detection", () => {
  var server, env, receivedRequests;

  beforeAll(() => {
    receivedRequests = [];
    server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        receivedRequests.push({
          method: req.method,
          contentType: req.headers.get("content-type"),
          accept: req.headers.get("accept"),
          body: await req.text(),
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

  test("JSON object body gets application/json", async () => {
    sendWithBody(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/`,
      '{"query":"SELECT 1"}',
    );
    await drainRequest(env.control);

    var last = receivedRequests.at(-1);
    expect(last.contentType).toBe("application/json");
    expect(last.accept).toBe("application/vnd.apache.arrow.stream");
  });

  test("JSON array body gets application/json", async () => {
    sendWithBody(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/`,
      "[1, 2, 3]",
    );
    await drainRequest(env.control);

    var last = receivedRequests.at(-1);
    expect(last.contentType).toBe("application/json");
  });

  test("plain text body gets text/plain", async () => {
    sendWithBody(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/`,
      "SELECT * FROM users",
    );
    await drainRequest(env.control);

    var last = receivedRequests.at(-1);
    expect(last.contentType).toBe("text/plain");
  });

  test("GET request (no body) sets Accept header for Arrow", async () => {
    sendRequest(env.control, env.urlBytes, `http://localhost:${server.port}/`);
    await drainRequest(env.control);

    var last = receivedRequests.at(-1);
    expect(last.method).toBe("GET");
    expect(last.accept).toBe("application/vnd.apache.arrow.stream");
    expect(last.contentType).toBeNull();
  });
});
