/// <reference lib="webworker" />

declare function importScripts(...urls: string[]): void;

import {
  BODY_CHUNK_REQUEST,
  CATALOG_REGION_SIZE,
  CONTROL_INTS,
  DATA_OFFSET,
  URL_CAPACITY,
  URL_OFFSET,
} from "../protocol";
import { applyHostDebugFlag, createLogger } from "../logger";
import { N6K_VERSION } from "../version";
import { createPushQueue } from "./push-queue";
import { createCatalogRegistry } from "./ws-registry";
import { WS_SAB_SIZE } from "../ws-sab";
import { createVsockRegistry } from "../virtual-socket/duckdb-glue";
import {
  channelBytes,
  DEFAULT_CHANNEL_LAYOUT,
} from "../virtual-socket/channel";

const log = createLogger("duckdb-worker");
import { N6K_PROTOCOL_VERSION, OP } from "../ws-framing";
import {
  wsReqTextBody,
  wsReqBinaryBody,
  wsReadResponseText,
  wsReadResponseIntoHeap,
  wsStartStreamAndWaitFirstChunk,
  wsStartStreamBinaryAndWaitFirstChunk,
  wsReadStreamChunkIntoHeap,
  wsAdvanceStreamAndWait,
  wsCancelStreamAndReleaseChannel,
  encodeUrlIntoSABUrlBuffer,
  encodeBodyIntoSABUrlBufferAfterUrl,
  storeZeroBodyLengthForGetRequest,
  storeNegativeOneBodyLengthForDeleteRequest,
  storeNegativeBodyLengthForBinaryUpload,
  clearStoredBodyLength,
  dispatchSABRequestAndBlockUntilFetchWorkerResponds,
  returnErrorStringIfResponseFailedAndReleaseChannel,
  decodeResponseTextFromSABDataBuffer,
  storeFlagIdleToReleaseSABChannel,
  loadResponseFlagLengthAndStatusFromSAB,
  readAllResponseChunksFromSABAsText,
  copyAllResponseChunksFromSABIntoWASMHeap,
  encodeContentTypeHeaderIntoSABDataBuffer,
  waitForFetchWorkerToRequestBodyChunk,
  writeBodyChunkFromHeapIntoSABAndSignalReady,
  blockUntilFetchWorkerRespondsAfterBodyUpload,
  blockUntilConnectionSignalAndReturn,
} from "../atomics";

interface N6kInitMessage {
  type: "n6k-init";
  sab: SharedArrayBuffer;
  mainWorkerUrl: string;
  // Private port for n6k control frames, kept off duckdb-wasm's shared RPC channel.
  n6kPort: MessagePort;
  // Page debug snapshot; workers can't read localStorage.
  debug?: boolean;
}

// Per-catalog ws-SABs so concurrent catalogs never collide on one WS channel.
const wsSabs = createCatalogRegistry<SharedArrayBuffer>();

// Per-catalog vsock channels for the shared-WsClient path.
const vsock = createVsockRegistry();

// Resolve a ws spec into a dialable ws(s):// URL; relative specs resolve against the worker origin.
function resolveWsUrl(spec: string): string {
  if (spec.startsWith("ws://") || spec.startsWith("wss://")) return spec;
  if (spec.startsWith("https://")) return "wss://" + spec.slice(8);
  if (spec.startsWith("http://")) return "ws://" + spec.slice(7);
  if (spec.startsWith("n6ks://")) return "wss://" + spec.slice(7);
  if (spec.startsWith("n6k://")) return "ws://" + spec.slice(6);
  const u = new URL(spec, globalThis.location.origin);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  return u.toString();
}

// Per-catalog queue of server PUSH events, drained by the wasm extension.
const pushQueue = createPushQueue();

interface N6kApi {
  fetch(url: string): string;
  fetchBinary(url: string): string;
  post(url: string, body: string): string;
  postBinary(url: string, body: string): string;
  postMultipart(
    url: string,
    bodyPtr: number,
    bodyLen: number,
    boundary: string,
    heap: Uint8Array,
  ): string;
  attach(
    wsUrl: string,
    baseUrl: string,
    token: string,
    catalog: string,
  ): string;
  attachWs(wsId: string, catalog: string, token: string): string;
  detach(catalog: string): void;
  catalogList(catalog: string): string;
  wsReq(catalog: string, op: number, bodyStr: string): string;
  wsReqBinary(
    catalog: string,
    op: number,
    headerStr: string,
    bodyPtr: number,
    bodyLen: number,
    heap: Uint8Array,
  ): string;
  wsReadText(catalog: string): string;
  wsReadIntoHeap(catalog: string, ptr: number, heap: Uint8Array): number;
  wsStartStream(catalog: string, op: number, bodyStr: string): string;
  wsStartStreamBinary(
    catalog: string,
    op: number,
    headerStr: string,
    bodyPtr: number,
    bodyLen: number,
    heap: Uint8Array,
  ): string;
  wsReadChunkIntoHeap(catalog: string, ptr: number, heap: Uint8Array): number;
  wsAdvanceStream(catalog: string): string;
  wsCancelStream(catalog: string): void;
  // vsock: shared-WsClient transport; moves opaque n6k frames over the per-catalog channel.
  // ringPtr (coi extension): C++ malloc pointer to the channel inside duckdb's shared
  // wasm heap. Omitted by pre-rebuild extensions — then we fall back to a standalone SAB.
  vsockAttach(
    catalog: string,
    wsUrl: string,
    token: string,
    ringPtr?: number,
    doorbellPtr?: number,
  ): string;
  vsockAttachWs(
    catalog: string,
    wsId: string,
    token: string,
    ns?: number,
    ringPtr?: number,
    doorbellPtr?: number,
  ): string;
  vsockSend(catalog: string, ptr: number, len: number, heap: Uint8Array): void;
  vsockRecv(
    catalog: string,
    ptr: number,
    cap: number,
    heap: Uint8Array,
  ): number;
  vsockWait(catalog: string, timeoutMs: number): void;
  vsockEpoch(catalog: string): number;
  vsockClose(catalog: string): void;
  readIntoHeap(ptr: number, heap: Uint8Array): number;
  delete(url: string): string;
  takePushEvents(catalog: string): string;
}

interface N6kApiWithVersion extends N6kApi {
  version: number;
  npmVersion: string;
}

declare global {
  var n6k: N6kApiWithVersion;
}

globalThis.addEventListener("error", (e: ErrorEvent) => {
  log.error("uncaught error:", e.message || e);
  if (e.filename) log.error("at", e.filename + ":" + e.lineno + ":" + e.colno);
});

// The shared-memory data path needs the exact WebAssembly.Memory duckdb-wasm runs
// on. In the coi build that Memory is created inside the DuckDBModule factory
// closure (not on any global) and IMPORTED into the wasm instance, so we grab it
// off the imports object of the standard WebAssembly.instantiate(Streaming) call
// duckdb makes. Installed before importScripts so it's in place when duckdb boots.
// (The production forwarder can instead export it from C++ via EM_JS, where
// `wasmMemory` is a visible closure var.)
let capturedWasmMemory: WebAssembly.Memory | undefined;
// duckdb-wasm's `free` (and `malloc`) export, grabbed off the instantiated instance.
// JS owns freeing the heap-backed ring region on detach (the pump-teardown timing is
// only known here), so C++ must NOT also free it — that would double-free.
let capturedWasmFree: ((ptr: number) => void) | undefined;
function memoryFromImports(imports: unknown): WebAssembly.Memory | undefined {
  if (!imports || typeof imports !== "object") return undefined;
  for (const ns of Object.values(imports as Record<string, unknown>)) {
    if (ns instanceof WebAssembly.Memory) return ns;
    if (ns && typeof ns === "object") {
      for (const v of Object.values(ns as Record<string, unknown>)) {
        if (v instanceof WebAssembly.Memory) return v;
      }
    }
  }
  return undefined;
}
// Pull the wasm `free` export off the resolved instantiate result (instance.exports.free
// or ._free). emscripten exports these because C++ uses them everywhere.
function captureExports<T>(result: T): T {
  const r = result as unknown as
    | { instance?: WebAssembly.Instance }
    | WebAssembly.Instance;
  const inst = (r as { instance?: WebAssembly.Instance }).instance ?? r;
  const exports = (inst as WebAssembly.Instance).exports as
    | Record<string, unknown>
    | undefined;
  const free = exports?.free ?? exports?._free;
  if (typeof free === "function") {
    capturedWasmFree = free as (ptr: number) => void;
  }
  return result;
}
{
  const origInstantiate = WebAssembly.instantiate;
  WebAssembly.instantiate = function (
    this: unknown,
    ...args: Parameters<typeof WebAssembly.instantiate>
  ) {
    const mem = memoryFromImports(args[1]);
    if (mem) capturedWasmMemory = mem;
    return origInstantiate.apply(this, args).then(captureExports);
  } as typeof WebAssembly.instantiate;
  if (WebAssembly.instantiateStreaming) {
    const origStreaming = WebAssembly.instantiateStreaming;
    WebAssembly.instantiateStreaming = function (
      this: unknown,
      ...args: Parameters<typeof WebAssembly.instantiateStreaming>
    ) {
      const mem = memoryFromImports(args[1]);
      if (mem) capturedWasmMemory = mem;
      return origStreaming.apply(this, args).then(captureExports);
    } as typeof WebAssembly.instantiateStreaming;
  }
}

// Open a vsock channel for a catalog and register the duckdb-side endpoint. Heap-backed
// when the coi extension passed a ring pointer AND we captured duckdb's shared Memory;
// otherwise a standalone SharedArrayBuffer (bootstrap: JS shipped before the rebuild).
// Returns the carrier fields to forward to the pump ws-worker — the discriminator the
// ws-worker uses to rebuild the same rings on its side.
// doorbellOffset (heap path only): wasm-heap byte offset of the shared n6k I/O thread's doorbell
// int32; the pump bumps + notifies it after publishing an inbound frame so the one I/O thread wakes.
// 0 (or absent) when the extension predates the shared I/O thread — then the pump rings nothing extra.
type VsockCarrier =
  | {
      wasmMemory: WebAssembly.Memory;
      ringOffset: number;
      doorbellOffset: number;
    }
  | { channelSab: SharedArrayBuffer };
// How each catalog's vsock channel was backed, for the n6k-vsock-selftest probe:
// heap == the coi extension supplied a ring pointer into duckdb's shared heap;
// !heap == the standalone-SAB bootstrap (extension not yet rebuilt / not threaded).
const vsockAttachInfo = new Map<
  string,
  { heap: boolean; ringOffset: number }
>();
function openVsockChannel(
  catalog: string,
  ringPtr?: number,
  doorbellPtr?: number,
): VsockCarrier {
  if (ringPtr && ringPtr > 0 && capturedWasmMemory) {
    vsock.register(catalog, capturedWasmMemory.buffer, undefined, ringPtr);
    vsockAttachInfo.set(catalog, { heap: true, ringOffset: ringPtr });
    return {
      wasmMemory: capturedWasmMemory,
      ringOffset: ringPtr,
      doorbellOffset: doorbellPtr ?? 0,
    };
  }
  const sab = new SharedArrayBuffer(channelBytes(DEFAULT_CHANNEL_LAYOUT));
  vsock.register(catalog, sab);
  vsockAttachInfo.set(catalog, { heap: false, ringOffset: 0 });
  return { channelSab: sab };
}

globalThis.addEventListener(
  "message",
  function _n6kInitHandler(e: MessageEvent) {
    if (!(e.data && e.data.type === "n6k-init")) return;
    const { sab, mainWorkerUrl, n6kPort, debug } = e.data as N6kInitMessage;
    applyHostDebugFlag(debug);

    const ctrl = new Int32Array(sab, 0, CONTROL_INTS);
    const urlBuf = new Uint8Array(sab, URL_OFFSET, URL_CAPACITY);
    // Must stop short of the catalog/token region at the tail, which fetch-worker.ts owns --
    // postMultipart sizes chunks by dataBuf.length, so an over-long view corrupts it.
    const dataBuf = new Uint8Array(
      sab,
      DATA_OFFSET,
      sab.byteLength - DATA_OFFSET - CATALOG_REGION_SIZE,
    );
    const enc = new TextEncoder();
    const dec = new TextDecoder();

    const api: N6kApiWithVersion = {
      version: N6K_PROTOCOL_VERSION,
      npmVersion: N6K_VERSION,

      fetch(url: string): string {
        encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        storeZeroBodyLengthForGetRequest(ctrl);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        const result = decodeResponseTextFromSABDataBuffer(ctrl, dataBuf, dec);
        storeFlagIdleToReleaseSABChannel(ctrl);
        return result;
      },

      fetchBinary(url: string): string {
        encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        storeZeroBodyLengthForGetRequest(ctrl);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        const { respLen } = loadResponseFlagLengthAndStatusFromSAB(ctrl);
        return "OK:" + respLen;
      },

      post(url: string, body: string): string {
        const urlLen = encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        encodeBodyIntoSABUrlBufferAfterUrl(ctrl, urlBuf, body, urlLen, enc);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);
        clearStoredBodyLength(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        return readAllResponseChunksFromSABAsText(ctrl, dataBuf, dec);
      },

      postBinary(url: string, body: string): string {
        const urlLen = encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        encodeBodyIntoSABUrlBufferAfterUrl(ctrl, urlBuf, body, urlLen, enc);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);
        clearStoredBodyLength(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        const { respLen } = loadResponseFlagLengthAndStatusFromSAB(ctrl);
        return "OK:" + respLen;
      },

      attach(
        wsUrl: string,
        baseUrl: string,
        token: string,
        catalog: string,
      ): string {
        const sab = new SharedArrayBuffer(WS_SAB_SIZE);
        wsSabs.set(catalog, sab);
        n6kPort.postMessage({
          type: "n6k-attach",
          wsUrl,
          baseUrl,
          token,
          catalog,
          wsSab: sab,
        });
        const signal = blockUntilConnectionSignalAndReturn(ctrl);
        if (signal === 2) {
          wsSabs.remove(catalog);
          return "ATTACH_ERROR: WebSocket connection failed";
        }
        return "OK";
      },

      // Attach onto an app-registered socket by wsId instead of a URL (binary frames only).
      attachWs(wsId: string, catalog: string, token: string): string {
        const sab = new SharedArrayBuffer(WS_SAB_SIZE);
        wsSabs.set(catalog, sab);
        n6kPort.postMessage({
          type: "n6k-attach-ws",
          wsId,
          token,
          catalog,
          wsSab: sab,
        });
        const signal = blockUntilConnectionSignalAndReturn(ctrl);
        if (signal === 2) {
          wsSabs.remove(catalog);
          return "ATTACH_ERROR: WebSocket connection failed";
        }
        return "OK";
      },

      detach(catalog: string): void {
        wsSabs.remove(catalog);
        n6kPort.postMessage({ type: "n6k-detach", catalog });
      },

      takePushEvents(catalog: string): string {
        return pushQueue.take(catalog);
      },

      catalogList(catalog: string): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        const r = wsReqTextBody(sab, OP.CATALOG_LIST, new Uint8Array(), dec);
        if (r.startsWith("WS_ERROR:")) return r;
        return wsReadResponseText(sab, dec);
      },

      wsReq(catalog: string, op: number, bodyStr: string): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        return wsReqTextBody(sab, op, enc.encode(bodyStr), dec);
      },

      wsReqBinary(
        catalog: string,
        op: number,
        headerStr: string,
        bodyPtr: number,
        bodyLen: number,
        heap: Uint8Array,
      ): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        return wsReqBinaryBody(
          sab,
          op,
          enc.encode(headerStr + "\n"),
          bodyPtr,
          bodyLen,
          heap,
          dec,
        );
      },

      wsReadText(catalog: string): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "";
        return wsReadResponseText(sab, dec);
      },

      wsReadIntoHeap(catalog: string, ptr: number, heap: Uint8Array): number {
        const sab = wsSabs.get(catalog);
        if (!sab) return 0;
        return wsReadResponseIntoHeap(sab, ptr, heap);
      },

      wsStartStream(catalog: string, op: number, bodyStr: string): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        return wsStartStreamAndWaitFirstChunk(
          sab,
          op,
          enc.encode(bodyStr),
          dec,
        );
      },

      wsStartStreamBinary(
        catalog: string,
        op: number,
        headerStr: string,
        bodyPtr: number,
        bodyLen: number,
        heap: Uint8Array,
      ): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        return wsStartStreamBinaryAndWaitFirstChunk(
          sab,
          op,
          enc.encode(headerStr + "\n"),
          bodyPtr,
          bodyLen,
          heap,
          dec,
        );
      },

      wsReadChunkIntoHeap(
        catalog: string,
        ptr: number,
        heap: Uint8Array,
      ): number {
        const sab = wsSabs.get(catalog);
        if (!sab) return 0;
        return wsReadStreamChunkIntoHeap(sab, ptr, heap);
      },

      wsAdvanceStream(catalog: string): string {
        const sab = wsSabs.get(catalog);
        if (!sab) return "WS_ERROR:no ws-SAB for catalog " + catalog;
        return wsAdvanceStreamAndWait(sab, dec);
      },

      wsCancelStream(catalog: string): void {
        const sab = wsSabs.get(catalog);
        if (!sab) return;
        wsCancelStreamAndReleaseChannel(sab);
      },

      // Open the channel (heap-backed or bootstrap SAB), spawn a byte-pump ws-worker;
      // returns immediately — HELLO buffers in the TX ring.
      vsockAttach(
        catalog: string,
        wsUrl: string,
        token: string,
        ringPtr?: number,
        doorbellPtr?: number,
      ): string {
        const carrier = openVsockChannel(catalog, ringPtr, doorbellPtr);
        n6kPort.postMessage({
          type: "n6k-vsock-attach",
          catalog,
          wsUrl: resolveWsUrl(wsUrl),
          token,
          ...carrier,
        });
        // Signal which backing was chosen so the C++ side knows whether it may drive the ring
        // directly (heap) or must fall back to the JS-glue reactor (sab). Both keep the "OK"
        // prefix the C++ failure check (setup_res.rfind("OK", 0)) relies on.
        return "wasmMemory" in carrier ? "OK-heap" : "OK-sab";
      },

      // As vsockAttach but adopts an app socket by wsId; ns is the session id so one socket carries many catalogs.
      vsockAttachWs(
        catalog: string,
        wsId: string,
        token: string,
        ns: number = 0,
        ringPtr?: number,
        doorbellPtr?: number,
      ): string {
        const carrier = openVsockChannel(catalog, ringPtr, doorbellPtr);
        n6kPort.postMessage({
          type: "n6k-vsock-attach-ws",
          catalog,
          wsId,
          token,
          ns,
          ...carrier,
        });
        // See vsockAttach: "OK-heap" = direct reactor, "OK-sab" = JS-glue fallback.
        return "wasmMemory" in carrier ? "OK-heap" : "OK-sab";
      },

      vsockSend(
        catalog: string,
        ptr: number,
        len: number,
        heap: Uint8Array,
      ): void {
        vsock.send(catalog, ptr, len, heap);
      },
      vsockRecv(
        catalog: string,
        ptr: number,
        cap: number,
        heap: Uint8Array,
      ): number {
        return vsock.recv(catalog, ptr, cap, heap);
      },
      vsockWait(catalog: string, timeoutMs: number): void {
        vsock.wait(catalog, timeoutMs);
      },
      vsockEpoch(catalog: string): number {
        return vsock.epoch(catalog);
      },
      vsockClose(catalog: string): void {
        vsock.close(catalog);
        n6kPort.postMessage({ type: "n6k-vsock-detach", catalog });
      },

      postMultipart(
        url: string,
        bodyPtr: number,
        bodyLen: number,
        boundary: string,
        heap: Uint8Array,
      ): string {
        encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        storeNegativeBodyLengthForBinaryUpload(ctrl, bodyLen);
        encodeContentTypeHeaderIntoSABDataBuffer(ctrl, dataBuf, boundary, enc);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);

        let offset = 0;
        while (offset < bodyLen) {
          const flag = waitForFetchWorkerToRequestBodyChunk(ctrl);
          if (flag !== BODY_CHUNK_REQUEST) break;
          const chunkSize = Math.min(bodyLen - offset, dataBuf.length);
          writeBodyChunkFromHeapIntoSABAndSignalReady(
            ctrl,
            dataBuf,
            heap,
            bodyPtr,
            offset,
            chunkSize,
            offset + chunkSize >= bodyLen,
          );
          offset += chunkSize;
        }

        blockUntilFetchWorkerRespondsAfterBodyUpload(ctrl);
        clearStoredBodyLength(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        const { respLen } = loadResponseFlagLengthAndStatusFromSAB(ctrl);
        return "OK:" + respLen;
      },

      delete(url: string): string {
        encodeUrlIntoSABUrlBuffer(ctrl, urlBuf, url, enc);
        storeNegativeOneBodyLengthForDeleteRequest(ctrl);
        dispatchSABRequestAndBlockUntilFetchWorkerResponds(ctrl);
        clearStoredBodyLength(ctrl);
        const err = returnErrorStringIfResponseFailedAndReleaseChannel(
          ctrl,
          dataBuf,
          dec,
        );
        if (err) return err;
        const result = decodeResponseTextFromSABDataBuffer(ctrl, dataBuf, dec);
        storeFlagIdleToReleaseSABChannel(ctrl);
        return result;
      },

      readIntoHeap(ptr: number, heap: Uint8Array): number {
        return copyAllResponseChunksFromSABIntoWASMHeap(
          ctrl,
          dataBuf,
          ptr,
          heap,
        );
      },
    };

    globalThis.n6k = new Proxy(api, {
      get(target, prop, receiver) {
        if (prop in target) return Reflect.get(target, prop, receiver);
        if (typeof prop === "string") {
          throw new TypeError(
            `n6k API mismatch: "${prop}" is not a method on the JS worker ` +
              `(protocol v${N6K_PROTOCOL_VERSION}). ` +
              `WASM extension and JS worker are out of sync — rebuild both.`,
          );
        }
        return;
      },
    });

    globalThis.removeEventListener("message", _n6kInitHandler);

    // Persistent listener (survives init) for host→worker control messages.
    globalThis.addEventListener("message", function (ev: MessageEvent) {
      const m = ev.data;
      if (!m) return;
      // Server PUSH events queued for the wasm extension to drain between C++ entry points.
      if (m.type === "n6k-push") {
        pushQueue.enqueue(m.catalog, m.op, m.body);
        return;
      }
      // Host teardown: drop per-catalog SABs and have the main thread terminate ws-workers.
      if (m.type === "n6k-detach") {
        wsSabs.clear();
        n6kPort.postMessage({ type: "n6k-detach" });
        return;
      }
      // Reclaim a heap-backed ring region. The driver sends this only AFTER the pump
      // ws-worker has released the ring (its runPump exited), so no one still reads/writes
      // the region and freeing it (returning it to duckdb's allocator) is safe.
      if (m.type === "n6k-vsock-free-ring") {
        // Drop the (now-closed) endpoint first so its stale ring views can't be reused,
        // then return the region to duckdb's allocator.
        if (typeof m.catalog === "string") vsock.unregister(m.catalog);
        const off = m.ringOffset;
        if (capturedWasmFree && typeof off === "number") capturedWasmFree(off);
        return;
      }
      // Diagnostic for the shared-memory data path: report duckdb-wasm's wasm heap
      // so callers can verify the coi build exposes a forwardable SharedArrayBuffer
      // (the substrate the RX/TX rings + pthread mailboxes live in). Reads
      // capturedWasmMemory, not Module.wasmMemory — the Memory is created inside the
      // DuckDBModule factory closure and never lands on a global, which is why the
      // WebAssembly.instantiate patch above exists. Reply on the transferred port so
      // duckdb-wasm's own message handler never observes the response.
      if (m.type === "n6k-probe-memory") {
        const port = m.replyPort as MessagePort | undefined;
        const buffer = capturedWasmMemory?.buffer;
        port?.postMessage({
          type: "n6k-probe-memory-result",
          found: capturedWasmMemory != null,
          isSharedArrayBuffer:
            typeof SharedArrayBuffer !== "undefined" &&
            buffer instanceof SharedArrayBuffer,
          byteLength: buffer?.byteLength ?? 0,
          source: capturedWasmMemory
            ? "WebAssembly.instantiate imports"
            : "none",
        });
        return;
      }
      // Selftest for the heap-backed data path: report whether a catalog's vsock
      // channel took the shared-heap path (ring offset into duckdb's Memory) vs the
      // standalone-SAB bootstrap. No `catalog` = the most recent attach. Reply on the
      // transferred port so duckdb-wasm's own handler never sees it.
      if (m.type === "n6k-vsock-selftest") {
        const port = m.replyPort as MessagePort | undefined;
        const catalog = m.catalog as string | undefined;
        const info = catalog
          ? vsockAttachInfo.get(catalog)
          : [...vsockAttachInfo.values()].at(-1);
        port?.postMessage({
          type: "n6k-vsock-selftest-result",
          attached: info != null,
          heap: info?.heap ?? false,
          ringOffset: info?.ringOffset ?? null,
          canFree: capturedWasmFree != null,
        });
        return;
      }
    });

    // Route duckdb-wasm's console through our debug-gated logger to silence duplicate RPC-error noise.
    console.log = (...args: unknown[]) => log.log(...args);
    console.info = (...args: unknown[]) => log.log(...args);
    console.debug = (...args: unknown[]) => log.debug(...args);
    console.warn = (...args: unknown[]) => log.warn(...args);
    console.error = (...args: unknown[]) => log.error(...args);

    log.debug("importScripts:", mainWorkerUrl);
    try {
      importScripts(mainWorkerUrl);
    } catch (error) {
      log.error("importScripts failed:", (error as Error).message || error);
      throw error;
    }
  },
);
