import {
  test,
  expect,
  afterAll,
  beforeAll,
  afterEach,
  beforeEach,
  describe,
} from "bun:test";
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
  ERROR,
  CHUNK_REQUEST,
  IDLE,
  DATA_OFFSET,
  CATALOG_REGION_SIZE,
} from "../protocol.js";
import { encodeCatalogData } from "../n6k-utils.js";

var dataCapacity = SAB_SIZE - DATA_OFFSET - CATALOG_REGION_SIZE;

function createWorkerEnv() {
  var sab = new SharedArrayBuffer(SAB_SIZE);
  var control = new Int32Array(sab, 0, 8);
  var urlBytes = new Uint8Array(sab, 32, 4096);
  var dataBytes = new Uint8Array(sab, DATA_OFFSET, dataCapacity);
  var catalogLenView = new Int32Array(sab, SAB_SIZE - CATALOG_REGION_SIZE, 2);
  var catalogRegion = new Uint8Array(
    sab,
    SAB_SIZE - CATALOG_REGION_SIZE + 8,
    CATALOG_REGION_SIZE - 8,
  );
  var worker = new Worker(
    new URL("../workers/fetch-worker.ts", import.meta.url),
  );
  worker.postMessage({ type: "init", sab });
  return {
    sab,
    control,
    urlBytes,
    dataBytes,
    worker,
    catalogLenView,
    catalogRegion,
  };
}

async function waitFlag(control, index, oldVal) {
  await Atomics.waitAsync(control, index, oldVal).value;
}

function sendRequest(control, urlBytes, url, body) {
  var enc = new TextEncoder();
  var encoded = enc.encode(url);
  urlBytes.set(encoded);
  Atomics.store(control, URL_LEN, encoded.length);
  if (body) {
    var bodyEncoded = enc.encode(body);
    urlBytes.set(bodyEncoded, encoded.length);
    Atomics.store(control, BODY_LEN, bodyEncoded.length);
  } else {
    Atomics.store(control, BODY_LEN, 0);
  }
  Atomics.store(control, FLAG, REQUEST);
  Atomics.notify(control, FLAG);
}

function readText(dataBytes, len) {
  return new TextDecoder().decode(dataBytes.slice(0, len));
}

async function assertWorkerReady(env, origin) {
  sendRequest(env.control, env.urlBytes, origin + "/__ready");
  await waitFlag(env.control, FLAG, REQUEST);
  var flag = Atomics.load(env.control, FLAG);
  Atomics.store(env.control, FLAG, IDLE);
  Atomics.notify(env.control, FLAG);
  if (flag !== RESPONSE_READY) {
    throw new Error(
      "fetch worker not in expected state: expected RESPONSE_READY (" +
        RESPONSE_READY +
        "), got flag=" +
        flag,
    );
  }
}

describe("fetch worker — happy path", () => {
  var server, env;

  beforeAll(() => {
    server = Bun.serve({
      port: 0,
      fetch: () => new Response("hello", { status: 200 }),
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("returns response through SAB", async () => {
    sendRequest(env.control, env.urlBytes, `http://localhost:${server.port}/`);
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

    Atomics.store(env.control, CHUNK_OFFSET, 0);
    Atomics.store(env.control, FLAG, CHUNK_REQUEST);
    Atomics.notify(env.control, FLAG);
    await waitFlag(env.control, FLAG, CHUNK_REQUEST);

    var chunkLen = Atomics.load(env.control, CHUNK_LEN);
    expect(readText(env.dataBytes, chunkLen)).toBe("hello");

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });
});

describe("fetch worker — error responses", () => {
  var server, env;

  beforeAll(() => {
    server = Bun.serve({
      port: 0,
      fetch: (req) => {
        var url = new URL(req.url);
        if (url.pathname === "/500")
          return new Response("internal server error", { status: 500 });
        if (url.pathname === "/404")
          return new Response("not found", { status: 404 });
        return new Response("ok", { status: 200 });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("HTTP 500 sets ERROR flag with body", async () => {
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/500`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(ERROR);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(500);

    var respLen = Atomics.load(env.control, RESP_LEN);
    expect(readText(env.dataBytes, respLen)).toBe("internal server error");

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });

  test("HTTP 404 sets ERROR flag", async () => {
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/404`,
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(ERROR);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(404);

    var respLen = Atomics.load(env.control, RESP_LEN);
    expect(readText(env.dataBytes, respLen)).toBe("not found");

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });
});

describe("fetch worker — POST", () => {
  var server, env, receivedRequests;

  beforeAll(() => {
    receivedRequests = [];
    server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        receivedRequests.push({
          method: req.method,
          contentType: req.headers.get("content-type"),
          body: await req.text(),
        });
        return new Response("accepted", { status: 200 });
      },
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("sends POST with body and Content-Type", async () => {
    sendRequest(
      env.control,
      env.urlBytes,
      `http://localhost:${server.port}/`,
      "SELECT * FROM test",
    );
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(200);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedRequests.length).toBe(1);
    expect(receivedRequests[0].method).toBe("POST");
    expect(receivedRequests[0].contentType).toBe("text/plain");
    expect(receivedRequests[0].body).toBe("SELECT * FROM test");
  });
});

describe("fetch worker — catalog headers", () => {
  var server, env, receivedHeaders, origin;

  beforeEach(async () => {
    receivedHeaders = [];
    server = Bun.serve({
      port: 0,
      fetch: (req) => {
        receivedHeaders.push({
          auth: req.headers.get("authorization"),
        });
        return new Response("ok", { status: 200 });
      },
    });
    // Use server.url.origin — an unbound/0 port would yield a URL fetch() rejects (ERR_INVALID_URL).
    origin = server.url.origin;
    env = createWorkerEnv();
    await assertWorkerReady(env, origin);
    receivedHeaders.length = 0;
  });

  afterEach(() => {
    env.worker.terminate();
    server.stop(true);
  });

  test("sends bearer token per catalog", async () => {
    var baseUrl = origin + "/app1";
    var catalogs = new Map([[baseUrl, { t: "my-token" }]]);
    encodeCatalogData(
      catalogs,
      env.catalogLenView,
      env.catalogRegion,
      new TextEncoder(),
    );

    sendRequest(env.control, env.urlBytes, baseUrl + "/tables");
    await waitFlag(env.control, FLAG, REQUEST);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedHeaders.length).toBe(1);
    expect(receivedHeaders[0].auth).toBe("Bearer my-token");
  });

  test("multiple catalogs get their own token", async () => {
    var base1 = origin + "/app1";
    var base2 = origin + "/app2";
    var catalogs = new Map([
      [base1, { t: "tok1" }],
      [base2, { t: "tok2" }],
    ]);
    encodeCatalogData(
      catalogs,
      env.catalogLenView,
      env.catalogRegion,
      new TextEncoder(),
    );

    sendRequest(env.control, env.urlBytes, base1 + "/tables");
    await waitFlag(env.control, FLAG, REQUEST);
    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
    await new Promise((r) => setTimeout(r, 10));

    sendRequest(env.control, env.urlBytes, base2 + "/schemas");
    await waitFlag(env.control, FLAG, REQUEST);
    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedHeaders.length).toBe(2);
    expect(receivedHeaders[0].auth).toBe("Bearer tok1");
    expect(receivedHeaders[1].auth).toBe("Bearer tok2");
  });

  test("no auth header when catalog data is empty", async () => {
    sendRequest(env.control, env.urlBytes, origin + "/");
    await waitFlag(env.control, FLAG, REQUEST);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);

    expect(receivedHeaders.length).toBe(1);
    expect(receivedHeaders[0].auth).toBeNull();
  });
});

describe("fetch worker — network error", () => {
  var env;

  beforeAll(() => {
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
  });

  test("unreachable host sets ERROR with status 0", async () => {
    sendRequest(env.control, env.urlBytes, "http://127.0.0.1:1/unreachable");
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(ERROR);
    expect(Atomics.load(env.control, HTTP_STATUS)).toBe(0);

    var respLen = Atomics.load(env.control, RESP_LEN);
    expect(respLen).toBeGreaterThan(0);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });
});

describe("fetch worker — chunking", () => {
  var server, env, largeBody;

  beforeAll(() => {
    largeBody = "A".repeat(dataCapacity + 1000);
    server = Bun.serve({
      port: 0,
      fetch: () => new Response(largeBody, { status: 200 }),
    });
    env = createWorkerEnv();
  });

  afterAll(() => {
    env.worker.terminate();
    server.stop();
  });

  test("handles response larger than data buffer via chunks", async () => {
    sendRequest(env.control, env.urlBytes, `http://localhost:${server.port}/`);
    await waitFlag(env.control, FLAG, REQUEST);

    expect(Atomics.load(env.control, FLAG)).toBe(RESPONSE_READY);
    var totalLen = Atomics.load(env.control, RESP_LEN);
    expect(totalLen).toBe(largeBody.length);

    var chunks = [];
    var offset = 0;
    while (offset < totalLen) {
      Atomics.store(env.control, CHUNK_OFFSET, offset);
      Atomics.store(env.control, FLAG, CHUNK_REQUEST);
      Atomics.notify(env.control, FLAG);
      await waitFlag(env.control, FLAG, CHUNK_REQUEST);

      var chunkLen = Atomics.load(env.control, CHUNK_LEN);
      expect(chunkLen).toBeGreaterThan(0);
      expect(chunkLen).toBeLessThanOrEqual(dataCapacity);
      chunks.push(readText(env.dataBytes, chunkLen));
      offset += chunkLen;
    }

    expect(chunks.join("")).toBe(largeBody);
    expect(chunks.length).toBeGreaterThan(1);

    Atomics.store(env.control, FLAG, IDLE);
    Atomics.notify(env.control, FLAG);
  });
});
