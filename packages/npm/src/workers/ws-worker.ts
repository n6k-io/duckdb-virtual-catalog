// One ws-worker per attach: internal (owns a WebSocket) or external (app socket bridged).
// SAB mode drives requests via the ws-SAB lock-step; parent-driven mode is for tests.

import {
  FT,
  OP,
  N6K_PROTOCOL_VERSION,
  type Frame,
  type FrameHeader,
  packFrame,
  unpackFrame,
} from "../ws-framing";
import {
  WS_FLAG,
  WS_OP,
  WS_BODY_LEN,
  WS_RESP_LEN,
  WS_IDLE,
  WS_REQ_READY,
  WS_RESP_READY,
  WS_ERR,
  WS_CHUNK_READY,
  WS_CHUNK_NEXT,
  WS_STREAM_END,
  WS_RESP_CAPACITY,
  wsSabViews,
} from "../ws-sab";
import { applyHostDebugFlag, createLogger } from "../logger";
import { wsWorkerEndpoint, type Endpoint } from "../virtual-socket/channel";
import {
  runPump,
  acceptInbound,
  createInboundBuffer,
  type InboundBuffer,
} from "../virtual-socket/pump";

const log = createLogger("ws-worker");

// Byte-pump mode: owns the real WebSocket, moves opaque frames to/from the vsock channel; no parsing.
let pumpEndpoint: Endpoint | null = null;
let pumpStopped = false;
// Overflow buffer for inbound frames that didn't fit the RX ring (see pump.ts).
let pumpInbound: InboundBuffer = createInboundBuffer();

let ws: WebSocket | null = null;
// Outbound seam: internal sends on the WebSocket, external relays via postMessage.
type Transport = { send: (frame: Uint8Array) => void; isOpen: () => boolean };
let transport: Transport | null = null;
let externalOpen = false;
// Bearer token, sent in the FT_HELLO handshake (keeps it out of the URL/logs).
let authToken: string | null = null;
let helloSent = false;
let helloReady = false;
let helloReadyResolvers: Array<() => void> = [];
// Session-ready gate, distinct from HELLO_ACK: session_pending stays closed until FT_READY.
let sessionReady = false;
let sessionReadyResolvers: Array<() => void> = [];
let socketClosed = false;
let lastCloseReason = "";
// Set when this worker is retired; the SAB poll loop exits so a successor can take the shared SAB.
let stopped = false;
// Catalog name; tags PUSH events for main-thread routing.
let catalogName: string | null = null;

// Per-reqId collectors: SAB mode assembles a full response before writing back.
type Pending = {
  jsonChunk: string | null; // first non-Arrow RESP_CHUNK payload
  arrow: Uint8Array[]; // RESP_SCHEMA + arrow RESP_CHUNK payloads
  end?: { ok: boolean; payload: Uint8Array };
  err?: string;
};
const pending = new Map<number, Pending>();
const waiters = new Map<number, (p: Pending) => void>();

// Per-reqId streaming queue, one frame per entry in order (SCAN/QUERY).
type StreamFrame =
  | { kind: "chunk"; payload: Uint8Array }
  | { kind: "end" }
  | { kind: "err"; message: string };
const streamQueues = new Map<number, StreamFrame[]>();
const streamWaiters = new Map<number, (f: StreamFrame) => void>();

function pushStreamFrame(reqId: number, frame: StreamFrame): void {
  const w = streamWaiters.get(reqId);
  if (w) {
    streamWaiters.delete(reqId);
    w(frame);
    return;
  }
  let q = streamQueues.get(reqId);
  if (!q) {
    q = [];
    streamQueues.set(reqId, q);
  }
  q.push(frame);
}

function takeStreamFrame(reqId: number): Promise<StreamFrame> {
  const q = streamQueues.get(reqId);
  if (q && q.length > 0) {
    return Promise.resolve(q.shift()!);
  }
  return new Promise((resolve) => streamWaiters.set(reqId, resolve));
}

let sabMode: {
  sab: SharedArrayBuffer;
  ctrl: Int32Array;
  body: Uint8Array;
  resp: Uint8Array;
} | null = null;

let nextReqId = 1;

function postErr(message: string) {
  (globalThis as unknown as Worker).postMessage({ type: "error", message });
}

const TEXT_DECODER = new TextDecoder();
const EMPTY = new Uint8Array();
// Ops whose SAB body is `json-header\n + Arrow bytes` (rest are plain JSON).
const BINARY_OPS = new Set<number>([OP.INSERT, OP.RPC_TABLE]);

function sendBytes(frame: Uint8Array) {
  if (!transport || !transport.isOpen()) {
    postErr("cannot send frame: socket not open");
    return;
  }
  transport.send(frame);
}

function sendFrame(header: FrameHeader, body?: Uint8Array) {
  sendBytes(packFrame(header, body));
}

// Translate SAB/parent request encoding (op + JSON, or json-header\n+Arrow) into a REQ frame.
function buildReqFrame(
  reqId: number,
  op: number,
  reqBody: Uint8Array,
): Uint8Array {
  let args: Record<string, unknown> = {};
  let body: Uint8Array = EMPTY;
  if (BINARY_OPS.has(op)) {
    const nl = reqBody.indexOf(0x0a); // '\n' splits json-header from Arrow bytes
    if (nl !== -1) {
      const head = TEXT_DECODER.decode(reqBody.subarray(0, nl));
      if (head) args = JSON.parse(head);
      body = reqBody.subarray(nl + 1);
    } else if (reqBody.byteLength > 0) {
      args = JSON.parse(TEXT_DECODER.decode(reqBody));
    }
  } else if (reqBody.byteLength > 0) {
    args = JSON.parse(TEXT_DECODER.decode(reqBody));
  }
  return packFrame({ t: FT.REQ, id: reqId, op, ...args }, body);
}

function onBinary(buf: ArrayBuffer) {
  let frame: Frame;
  try {
    frame = unpackFrame(buf);
  } catch (error) {
    postErr(`unpackFrame failed: ${(error as Error).message}`);
    return;
  }

  const h = frame.header;
  const reqId = typeof h.id === "number" ? h.id : 0;

  if (h.t === FT.HELLO_ACK && !helloSent) {
    helloSent = true;
    // Fail fast on a wire-incompatible server; there is no cross-version interop.
    if (h.protocol_version !== N6K_PROTOCOL_VERSION) {
      const message = `protocol version mismatch: server ${h.protocol_version}, client ${N6K_PROTOCOL_VERSION}`;
      socketClosed = true;
      lastCloseReason = message;
      surfaceSocketError(message);
      return;
    }
    helloReady = true;
    // session_pending: server defers a slow build, sends FT_READY later.
    const sessionPending = h.session_pending === true;
    (globalThis as unknown as Worker).postMessage({
      type: "ready",
      protocolVersion: h.protocol_version,
      maxConcurrentReqs: h.max_concurrent_reqs,
      defaultBatchCredits: h.default_batch_credits,
    });
    for (const r of helloReadyResolvers) r();
    helloReadyResolvers = [];
    log.debug(
      `HELLO_ACK: catalog=${catalogName} sessionPending=${sessionPending}`,
    );
    if (!sessionPending) {
      sessionReady = true;
      for (const r of sessionReadyResolvers) r();
      sessionReadyResolvers = [];
    }
    return;
  }

  if (h.t === FT.READY) {
    // Deferred build finished; release requests blocked on the ready-gate.
    log.debug(`FT_READY: catalog=${catalogName} (session now usable)`);
    sessionReady = true;
    for (const r of sessionReadyResolvers) r();
    sessionReadyResolvers = [];
    return;
  }

  if (h.t === FT.HELLO_ERR) {
    // Connection-scoped failure; surface the rich message so a waiter throws it, not "socket closed".
    let message = "connection rejected";
    if (h.exception_message) {
      message = h.exception_type
        ? `${h.exception_type}: ${h.exception_message}`
        : String(h.exception_message);
    }
    socketClosed = true;
    lastCloseReason = message;
    surfaceSocketError(message);
    return;
  }

  if (h.t === FT.PING) {
    sendFrame(h.id === undefined ? { t: FT.PONG } : { t: FT.PONG, id: h.id });
    return;
  }

  // Server PUSH (no reqId): forward to the parent tagged with catalog for routing.
  if (h.t === FT.PUSH) {
    const { t: _t, op, ...body } = h;
    void _t;
    (globalThis as unknown as Worker).postMessage({
      type: "push",
      catalog: catalogName,
      op,
      body,
    });
    return;
  }

  const terminal = h.t === FT.RESP_END || h.t === FT.ERR;
  // RESP_SCHEMA always carries Arrow; a RESP_CHUNK is Arrow only if arrow:true (else list in header).
  const isArrow = h.t === FT.RESP_SCHEMA || h.arrow === true;

  // SAB streaming mode: push each frame into the per-reqId queue as it arrives.
  if (sabMode && streamQueues.has(reqId)) {
    if (h.t === FT.ERR) {
      pushStreamFrame(reqId, { kind: "err", message: headerDataJson(h) });
    } else if (h.t === FT.RESP_END) {
      pushStreamFrame(reqId, { kind: "end" });
    } else if (isArrow) {
      pushStreamFrame(reqId, { kind: "chunk", payload: frame.body });
    }
    // Non-Arrow RESP_CHUNK on a streaming op shouldn't happen for SCAN/QUERY.
    return;
  }

  // SAB mode: accumulate frames per-reqId, notify the poller on end.
  if (sabMode && pending.has(reqId)) {
    const p = pending.get(reqId)!;
    switch (h.t) {
      case FT.RESP_SCHEMA:
      case FT.RESP_CHUNK: {
        if (isArrow) {
          p.arrow.push(frame.body);
        } else if (h.t === FT.RESP_CHUNK && p.jsonChunk === null) {
          // Catalog/tables list chunk: reconstruct the JSON from the header.
          p.jsonChunk = headerDataJson(h);
        }

        break;
      }
      case FT.RESP_END: {
        p.end = { ok: true, payload: EMPTY };

        break;
      }
      case FT.ERR: {
        p.err = headerDataJson(h);

        break;
      }
    }
    if (terminal) {
      const waiter = waiters.get(reqId);
      if (waiter) {
        waiters.delete(reqId);
        waiter(p);
      }
    }
    return;
  }

  // Parent-driven mode (tests): post an exact-fit owned copy so its buffer can be transferred.
  const bodyCopy = new Uint8Array(frame.body);
  (globalThis as unknown as Worker).postMessage(
    {
      type: "frame",
      reqId,
      frame: { header: h, body: bodyCopy },
      end: terminal,
    },
    [bodyCopy.buffer],
  );
}

// Reconstruct the JSON the wasm consumer parses from a header (all fields except t/id).
function headerDataJson(h: FrameHeader): string {
  const { t: _t, id: _id, ...rest } = h;
  void _t;
  void _id;
  return JSON.stringify(rest);
}

function awaitHello(): Promise<void> {
  if (helloReady) return Promise.resolve();
  return new Promise((r) => helloReadyResolvers.push(r));
}

// Block until the session is usable (HELLO_ACK or FT_READY); released early if the socket dies.
function awaitReady(): Promise<void> {
  if (sessionReady) return Promise.resolve();
  return new Promise((r) => sessionReadyResolvers.push(r));
}

function awaitReqEnd(reqId: number): Promise<Pending> {
  const p = pending.get(reqId);
  if (p && (p.end || p.err)) {
    pending.delete(reqId);
    return Promise.resolve(p);
  }
  return new Promise((resolve) => {
    waiters.set(reqId, (done) => {
      pending.delete(reqId);
      resolve(done);
    });
  });
}

function writeWsResp(bytes: Uint8Array) {
  if (!sabMode) return;
  if (bytes.byteLength > WS_RESP_CAPACITY) {
    writeWsErr(
      `response too large (${bytes.byteLength} > ${WS_RESP_CAPACITY})`,
    );
    return;
  }
  sabMode.resp.set(bytes, 0);
  Atomics.store(sabMode.ctrl, WS_RESP_LEN, bytes.byteLength);
  Atomics.store(sabMode.ctrl, WS_FLAG, WS_RESP_READY);
  Atomics.notify(sabMode.ctrl, WS_FLAG, 1);
}

function writeWsErr(msg: string) {
  if (!sabMode) return;
  const bytes = new TextEncoder().encode(msg);
  const n = Math.min(bytes.byteLength, WS_RESP_CAPACITY);
  sabMode.resp.set(bytes.subarray(0, n), 0);
  Atomics.store(sabMode.ctrl, WS_RESP_LEN, n);
  Atomics.store(sabMode.ctrl, WS_FLAG, WS_ERR);
  Atomics.notify(sabMode.ctrl, WS_FLAG, 1);
}

// Arrow-producing ops stream; runStreamingReq handles them uniformly.
const STREAMING_OPS = new Set<number>([
  OP.SCAN,
  OP.QUERY,
  OP.RPC_SCALAR,
  OP.RPC_TABLE,
]);

async function runStreamingReq(reqId: number, op: number, reqBody: Uint8Array) {
  if (!sabMode) return;
  const { ctrl, resp } = sabMode;

  streamQueues.set(reqId, []); // prime so onBinary routes here

  sendBytes(buildReqFrame(reqId, op, reqBody));

  try {
    while (true) {
      const frame = await takeStreamFrame(reqId);
      if (frame.kind === "err") {
        writeWsErr(`WS_ERROR: ${frame.message}`);
        return;
      }
      if (frame.kind === "end") {
        Atomics.store(ctrl, WS_FLAG, WS_STREAM_END);
        Atomics.notify(ctrl, WS_FLAG, 1);
        return;
      }
      if (frame.payload.byteLength > resp.byteLength) {
        writeWsErr(
          `WS_ERROR: chunk too large (${frame.payload.byteLength} > ${resp.byteLength})`,
        );
        return;
      }
      resp.set(frame.payload, 0);
      Atomics.store(ctrl, WS_RESP_LEN, frame.payload.byteLength);
      Atomics.store(ctrl, WS_FLAG, WS_CHUNK_READY);
      Atomics.notify(ctrl, WS_FLAG, 1);
      // Wait for duckdb-worker to consume and signal CHUNK_NEXT.
      const waitRes = Atomics.waitAsync(ctrl, WS_FLAG, WS_CHUNK_READY);
      if (waitRes.async) await waitRes.value;
      const observed = Atomics.load(ctrl, WS_FLAG);
      if (observed !== WS_CHUNK_NEXT) {
        // Consumer abandoned the stream (flag overwritten): cancel server-side and bail without touching SAB.
        sendFrame({ t: FT.CANCEL, id: reqId });
        return;
      }
      sendFrame({ t: FT.CREDIT, id: reqId, n: 1 }); // refill for the consumed chunk
    }
  } finally {
    streamQueues.delete(reqId);
    streamWaiters.delete(reqId);
  }
}

async function sabPollLoop() {
  if (!sabMode) return;
  const { ctrl, body } = sabMode;
  while (true) {
    // Wait for WS_REQ_READY; any other state means no REQ yet or the response isn't consumed.
    let flag = Atomics.load(ctrl, WS_FLAG);
    while (flag !== WS_REQ_READY) {
      const waitRes = Atomics.waitAsync(ctrl, WS_FLAG, flag);
      if (waitRes.async) {
        await waitRes.value;
      }
      if (stopped) return; // retired mid-wait: a successor owns the SAB now
      flag = Atomics.load(ctrl, WS_FLAG);
    }
    if (stopped) return;
    const op = Atomics.load(ctrl, WS_OP);
    const bodyLen = Atomics.load(ctrl, WS_BODY_LEN);
    const reqBody = new Uint8Array(bodyLen);
    reqBody.set(body.subarray(0, bodyLen));

    // Fail fast only on a closed socket, not a null transport (poll loop may start before "open").
    if (socketClosed) {
      log.debug(
        `request op=${op} failing fast: socket closed ` +
          `(catalog=${catalogName}, reason=${lastCloseReason || "no socket"})`,
      );
      writeWsErr(`WS_ERROR: socket closed: ${lastCloseReason || "no socket"}`);
      continue;
    }

    try {
      // Blocks until the session is usable or a drop releases the gate.
      if (!helloReady || !sessionReady) {
        log.debug(
          `request op=${op} waiting on session-ready gate ` +
            `(catalog=${catalogName}, helloReady=${helloReady}, ` +
            `sessionReady=${sessionReady})`,
        );
      }
      await awaitHello();
      // Wait can be long; re-check the socket after — must not send onto a dead one.
      await awaitReady();
      if (stopped) return;
      if (socketClosed || !transport) {
        writeWsErr(
          `WS_ERROR: socket closed: ${lastCloseReason || "no socket"}`,
        );
        continue;
      }
      const reqId = nextReqId++;

      if (STREAMING_OPS.has(op)) {
        await runStreamingReq(reqId, op, reqBody);
        continue;
      }

      pending.set(reqId, { jsonChunk: null, arrow: [] });
      sendBytes(buildReqFrame(reqId, op, reqBody));
      const p = await awaitReqEnd(reqId);
      if (p.err) {
        writeWsErr(`WS_ERROR: ${p.err}`);
      } else if (p.jsonChunk !== null) {
        writeWsResp(new TextEncoder().encode(p.jsonChunk));
      } else if (p.arrow.length > 0) {
        let total = 0;
        for (const a of p.arrow) total += a.byteLength;
        const out = new Uint8Array(total);
        let off = 0;
        for (const a of p.arrow) {
          out.set(a, off);
          off += a.byteLength;
        }
        writeWsResp(out);
      } else {
        writeWsResp(new Uint8Array());
      }
    } catch (error) {
      writeWsErr(`WS_ERROR: ${(error as Error).message}`);
    }
  }
}

function open(url: string, subprotocol?: string, token?: string | null) {
  if (ws) {
    postErr("ws-worker already opened");
    return;
  }
  authToken = token || null;
  log.debug(`dialing socket: catalog=${catalogName} url=${url}`);
  ws = subprotocol ? new WebSocket(url, subprotocol) : new WebSocket(url);
  ws.binaryType = "arraybuffer";
  transport = {
    send: (frame) => ws!.send(frame),
    isOpen: () => !!ws && ws.readyState === 1 /* OPEN */,
  };

  ws.addEventListener("open", function () {
    // Socket open. No HELLO_ACK after this = server accepted but never replied.
    log.debug(`socket open, sending HELLO: catalog=${catalogName}`);
    // FT_HELLO carries token (out of logs) + catalog; always sent, server replies HELLO_ACK or closes.
    sendFrame({
      t: FT.HELLO,
      token: authToken ?? "",
      catalog: catalogName ?? "",
    });
  });

  ws.addEventListener("message", function (ev: MessageEvent) {
    if (typeof ev.data === "string") {
      postErr(`unexpected text frame: ${ev.data}`);
      return;
    }
    onBinary(ev.data as ArrayBuffer);
  });

  ws.addEventListener("close", function (ev: CloseEvent) {
    const reason = ev.reason || `code=${ev.code}`;
    socketClosed = true;
    lastCloseReason = reason;
    surfaceSocketError(`socket closed: ${reason}`);
    (globalThis as unknown as Worker).postMessage({ type: "closed", reason });
    ws = null;
  });

  ws.addEventListener("error", function () {
    socketClosed = true;
    lastCloseReason = "ws error";
    surfaceSocketError("socket error");
    (globalThis as unknown as Worker).postMessage({
      type: "closed",
      reason: "ws error",
    });
  });
}

// External transport: app owns the socket, bridged as a byte pipe (ws-in/ws-out); binary frames only.
function openExternal(token?: string | null) {
  if (transport) {
    postErr("ws-worker already opened");
    return;
  }
  authToken = token || null;
  externalOpen = true;
  transport = {
    send: (frame) =>
      (globalThis as unknown as Worker).postMessage(
        { type: "ws-out", data: frame },
        [frame.buffer],
      ),
    isOpen: () => externalOpen,
  };
  // Socket already open: send FT_HELLO immediately.
  sendFrame({
    t: FT.HELLO,
    token: authToken ?? "",
    catalog: catalogName ?? "",
  });
}

// Start the byte pump; frames queued before open buffer in the TX ring and flush here.
function startPump() {
  if (!pumpEndpoint) return;
  void runPump(
    pumpEndpoint,
    { send: (frame) => sendBytes(frame) },
    pumpInbound,
    () => pumpStopped,
  ).then(() => {
    // runPump has exited, so its last ring access (final drain + the ringClose that
    // woke it) is done and nothing here touches the ring again. Tell the driver the
    // heap-backed region is safe to free — it gates the free on this ack.
    (globalThis as unknown as Worker).postMessage({
      type: "pump-ring-released",
    });
  });
}

// Forward one inbound frame without blocking; on overflow ceiling, tear down socket + channel.
function forwardPumpInbound(frame: Uint8Array) {
  if (!pumpEndpoint) return;
  if (acceptInbound(pumpEndpoint, pumpInbound, frame)) return;
  pumpStopped = true;
  pumpEndpoint.close(); // wakes a C++ WaitUntil parked on the channel
  try {
    ws?.close(1009, "n6k inbound overflow"); // 1009 = Message Too Big
  } catch {
    /* socket already closing */
  }
  (globalThis as unknown as Worker).postMessage({
    type: "closed",
    reason: "inbound overflow",
  });
  ws = null;
}

// Pump-mode dial: open the socket and wire it to the channel; C++ WsClient drives the protocol.
function openPump(url: string, subprotocol?: string) {
  if (ws) {
    postErr("ws-worker already opened");
    return;
  }
  log.debug(`dialing socket (pump): catalog=${catalogName} url=${url}`);
  ws = subprotocol ? new WebSocket(url, subprotocol) : new WebSocket(url);
  ws.binaryType = "arraybuffer";
  transport = {
    send: (frame) => ws!.send(frame),
    isOpen: () => !!ws && ws.readyState === 1 /* OPEN */,
  };
  ws.addEventListener("open", function () {
    log.debug(`socket open (pump): catalog=${catalogName}`);
    startPump();
    // Transport up; report "ready" so the catalog flips to connected.
    (globalThis as unknown as Worker).postMessage({ type: "ready" });
  });
  ws.addEventListener("message", function (ev: MessageEvent) {
    if (typeof ev.data === "string") return; // text belongs to the app
    forwardPumpInbound(new Uint8Array(ev.data as ArrayBuffer));
  });
  ws.addEventListener("close", function (ev: CloseEvent) {
    const reason = ev.reason || `code=${ev.code}`;
    pumpStopped = true;
    pumpEndpoint?.close(); // wakes a C++ WaitUntil parked on the channel
    (globalThis as unknown as Worker).postMessage({ type: "closed", reason });
    ws = null;
  });
  ws.addEventListener("error", function () {
    pumpStopped = true;
    pumpEndpoint?.close();
    (globalThis as unknown as Worker).postMessage({
      type: "closed",
      reason: "ws error",
    });
  });
}

// On socket death: release gates, fail all waiters, surface WS_ERR if SAB is mid-op.
function surfaceSocketError(message: string) {
  // Release gate waiters; they then observe socketClosed and fail fast.
  for (const r of helloReadyResolvers) r();
  helloReadyResolvers = [];
  for (const r of sessionReadyResolvers) r();
  sessionReadyResolvers = [];
  for (const reqId of streamQueues.keys()) {
    pushStreamFrame(reqId, { kind: "err", message });
  }
  for (const [reqId, state] of pending.entries()) {
    state.err = message;
    const waiter = waiters.get(reqId);
    if (waiter) {
      waiters.delete(reqId);
      waiter(state);
    }
  }
  // Surface WS_ERR if SAB is mid-flight on the consumer side.
  if (!sabMode) return;
  const { ctrl, resp } = sabMode;
  const flag = Atomics.load(ctrl, WS_FLAG);
  if (flag === WS_IDLE || flag === WS_ERR) return;
  const text = "WS_ERROR:" + message;
  const bytes = new TextEncoder().encode(text);
  const n = Math.min(bytes.byteLength, resp.byteLength);
  resp.set(bytes.subarray(0, n), 0);
  Atomics.store(ctrl, WS_RESP_LEN, n);
  Atomics.store(ctrl, WS_FLAG, WS_ERR);
  Atomics.notify(ctrl, WS_FLAG, 1);
}

(globalThis as unknown as Worker).addEventListener(
  "message",
  function (e: MessageEvent) {
    const msg = e.data;
    if (!msg || typeof msg !== "object") return;
    switch (msg.type) {
      case "init-sab": {
        applyHostDebugFlag(msg.debug);
        const sab = msg.sab as SharedArrayBuffer;
        if (typeof msg.catalog === "string") catalogName = msg.catalog;
        const views = wsSabViews(sab);
        sabMode = { sab, ...views };
        sabPollLoop();
        break;
      }
      case "init-channel": {
        applyHostDebugFlag(msg.debug);
        if (typeof msg.catalog === "string") catalogName = msg.catalog;
        // Heap-backed (coi extension): rings live in duckdb's shared WebAssembly.Memory
        // at the C++ malloc offset. Bootstrap: a standalone SAB. Same rings either way.
        pumpEndpoint = msg.wasmMemory
          ? wsWorkerEndpoint(
              (msg.wasmMemory as WebAssembly.Memory).buffer,
              undefined,
              msg.ringOffset as number,
            )
          : wsWorkerEndpoint(msg.channelSab as SharedArrayBuffer);
        // coi/threads: wake the shared n6k I/O thread by ringing its doorbell (the loop parks on it)
        // whenever this channel's RX ring changes state — after publishing an inbound frame, and on
        // close. send is the single publish choke point (acceptInbound/flushInbound/forwardPumpInbound
        // all route through it); ringing on close makes the I/O thread notice a drop promptly (it would
        // otherwise wait out its 250ms poll — long enough for a reconnect's ring reset to bump the
        // epoch first, so the stale registration reports "channel reset" instead of "socket closed" and
        // races the reconnected request). No doorbell on the SAB/glue path (no shared I/O thread).
        const doorbellOffset = (msg.doorbellOffset as number | undefined) ?? 0;
        if (msg.wasmMemory && doorbellOffset > 0) {
          const doorbell = new Int32Array(
            (msg.wasmMemory as WebAssembly.Memory).buffer,
            doorbellOffset,
            1,
          );
          const ringDoorbell = () => {
            Atomics.add(doorbell, 0, 1);
            Atomics.notify(doorbell, 0);
          };
          const base = pumpEndpoint;
          pumpEndpoint = {
            ...base,
            send: (frame: Uint8Array) => {
              const ok = base.send(frame);
              if (ok) ringDoorbell();
              return ok;
            },
            close: () => {
              base.close();
              ringDoorbell();
            },
          };
        }
        pumpInbound = createInboundBuffer(); // fresh per channel
        break;
      }
      case "open-pump": {
        openPump(msg.url, msg.subprotocol);
        break;
      }
      case "open-pump-external": {
        // Pump over an app-owned socket: send via ws-out, inbound via ws-in.
        externalOpen = true;
        transport = {
          send: (frame) =>
            (globalThis as unknown as Worker).postMessage(
              { type: "ws-out", data: frame },
              [frame.buffer],
            ),
          isOpen: () => externalOpen,
        };
        startPump();
        (globalThis as unknown as Worker).postMessage({ type: "ready" });
        break;
      }
      case "open": {
        open(msg.url, msg.subprotocol, msg.token);
        break;
      }
      case "open-external": {
        openExternal(msg.token);
        break;
      }
      case "ws-in": {
        // Inbound binary frame from the app's main-thread socket.
        if (pumpEndpoint) {
          forwardPumpInbound(new Uint8Array(msg.data as ArrayBuffer));
        } else {
          onBinary(msg.data as ArrayBuffer);
        }
        break;
      }
      case "ws-closed": {
        // App's socket dropped (external mode): mirror the internal "close".
        externalOpen = false;
        socketClosed = true;
        lastCloseReason = msg.reason || "app socket closed";
        if (pumpEndpoint) {
          pumpStopped = true;
          pumpEndpoint.close(); // wakes a C++ WaitUntil parked on the channel
        } else {
          surfaceSocketError(`socket closed: ${lastCloseReason}`);
        }
        (globalThis as unknown as Worker).postMessage({
          type: "closed",
          reason: lastCloseReason,
        });
        break;
      }
      case "req": {
        const body =
          msg.body instanceof Uint8Array ? msg.body : new Uint8Array(msg.body);
        sendBytes(buildReqFrame(msg.reqId >>> 0, msg.op & 0xff, body));
        break;
      }
      case "credit": {
        sendFrame({ t: FT.CREDIT, id: msg.reqId >>> 0, n: msg.n >>> 0 });
        break;
      }
      case "cancel": {
        sendFrame({ t: FT.CANCEL, id: msg.reqId >>> 0 });
        break;
      }
      case "close": {
        // Retire this worker: stop the poll loop, and (internal) send a WS close so the server drops its handler.
        stopped = true;
        externalOpen = false;
        try {
          ws?.close(1000, "detach");
        } catch {
          /* ignore */
        }
        break;
      }
      default: {
        postErr(`unknown message type: ${msg.type}`);
      }
    }
  },
);
