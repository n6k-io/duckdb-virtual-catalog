/* eslint-disable unicorn/prefer-global-this */
import { SAB_SIZE, CATALOG_REGION_SIZE } from "./protocol";
import { packFrame, FT } from "./ws-framing";
import { resetChannel } from "./virtual-socket/channel";
import { bindPump, deliverInbound } from "./virtual-socket/ws-demux";
import { encodeCatalogData } from "./n6k-utils";
import { createCatalogRegistry } from "./workers/ws-registry";
import type { CatalogEntry } from "./n6k-utils";
import {
  signalDuckDBWorkerConnectionSucceeded,
  signalDuckDBWorkerConnectionFailed,
} from "./atomics";
import { logger as log, isDebugEnabled } from "./logger";
import type { WsStatus } from "./types";

export type CreateN6kWorkerOpts = {
  mainWorkerUrl: string;
  fetchWorkerUrl: string | URL;
  duckdbWorkerUrl: string | URL;
  wsWorkerUrl: string | URL;
  onStatus?: (catalog: string, status: WsStatus) => void;
};

export type N6kWorkerHandle = {
  worker: Worker;
  // Reopen an attached catalog's socket without DETACH/ATTACH; no-op for unknown; status via onStatus.
  reconnect: (catalog: string) => void;
  // Register an open WebSocket, returning a wsId for ATTACH; driver claims only binary frames.
  registerWebsocket: (socket: WebSocket) => string;
  // Swap the socket behind a wsId; re-binds an attached catalog without DETACH/ATTACH (in-flight reqs fail).
  replaceWebsocket: (wsId: string, socket: WebSocket) => void;
  // Terminate the fetch + ws-workers and close the control channel. Does NOT touch the
  // duckdb `worker` (owned by AsyncDuckDB.terminate()); without it those handles pin the host loop.
  dispose: () => void;
};

export function createN6kWorker({
  mainWorkerUrl,
  fetchWorkerUrl,
  duckdbWorkerUrl,
  wsWorkerUrl,
  onStatus,
}: CreateN6kWorkerOpts): N6kWorkerHandle {
  if (typeof SharedArrayBuffer === "undefined") {
    throw new TypeError(
      "n6k requires cross-origin isolation (SharedArrayBuffer is not available). " +
        "crossOriginIsolated = " +
        (typeof self === "undefined" ? "unknown" : self.crossOriginIsolated),
    );
  }

  const notifyStatus = onStatus || function () {};

  // Snapshot page debug state and forward into every spawned worker (workers can't read localStorage).
  const debug = isDebugEnabled();

  const sab = new SharedArrayBuffer(SAB_SIZE);
  const control = new Int32Array(sab, 0, 8);

  const catalogLenView = new Int32Array(sab, SAB_SIZE - CATALOG_REGION_SIZE, 2);
  const catalogRegion = new Uint8Array(
    sab,
    SAB_SIZE - CATALOG_REGION_SIZE + 8,
    CATALOG_REGION_SIZE - 8,
  );
  const catalogEncoder = new TextEncoder();

  // Per-catalog connect params so reconnect() can reopen without DETACH/ATTACH (internal=wsUrl, external=wsId).
  type ConnectParams =
    | { external: false; wsUrl: string; token: string; sab: SharedArrayBuffer }
    | { external: true; wsId: string; token: string; sab: SharedArrayBuffer };
  const connectParams = new Map<string, ConnectParams>();

  // Ring-region carrier forwarded to the pump ws-worker: heap-backed (duckdb's shared
  // WebAssembly.Memory + the C++ malloc offset) or a standalone SAB (bootstrap). The
  // Memory object is structured-cloneable and shares by reference across postMessage.
  type ChannelCarrier =
    | {
        wasmMemory: WebAssembly.Memory;
        ringOffset: number;
        doorbellOffset: number;
      }
    | { channelSab: SharedArrayBuffer };
  const readCarrier = (m: {
    wasmMemory?: WebAssembly.Memory;
    ringOffset?: number;
    doorbellOffset?: number;
    channelSab?: SharedArrayBuffer;
  }): ChannelCarrier =>
    m.wasmMemory
      ? {
          wasmMemory: m.wasmMemory,
          ringOffset: m.ringOffset ?? 0,
          doorbellOffset: m.doorbellOffset ?? 0,
        }
      : { channelSab: m.channelSab as SharedArrayBuffer };
  // Reset both rings in place for a reconnect (buffer/offset unchanged; C++ owns the malloc).
  const resetCarrier = (c: ChannelCarrier): void =>
    "wasmMemory" in c
      ? resetChannel(c.wasmMemory.buffer, undefined, c.ringOffset)
      : resetChannel(c.channelSab);

  // Byte-pump (vsock) reconnect params per catalog so reconnect() can respawn the pump (external stores wsId).
  type PumpParams =
    | ({ external: false; wsUrl: string } & ChannelCarrier)
    | ({ external: true; wsId: string } & ChannelCarrier);
  const pumpParams = new Map<string, PumpParams>();
  const carrierOf = (pp: PumpParams): ChannelCarrier =>
    "wasmMemory" in pp
      ? {
          wasmMemory: pp.wasmMemory,
          ringOffset: pp.ringOffset,
          doorbellOffset: pp.doorbellOffset,
        }
      : { channelSab: pp.channelSab };

  // Heap-backed catalogs pending a ring-region free, keyed by catalog → wasm-heap offset.
  // Set on detach; the free fires only when that catalog's pump ws-worker acks it has
  // released the ring ("pump-ring-released"), so we never free a region still in use.
  // Bootstrap (SAB) catalogs never appear here — their buffer is just GC'd.
  const pendingRingFree = new Map<string, number>();

  // App-registered sockets by wsId; inbound listener attached at registration so early HELLO_ACK isn't missed.
  type ExternalSocket = {
    socket: WebSocket;
    // Frames buffered before a pump was bound to route them; replayed on bind.
    inbound: ArrayBuffer[];
    // Pumps on this one socket keyed by session ns (multiplex: one socket, N sessions).
    targets: Map<number, Worker>;
    onMessage: (ev: MessageEvent) => void;
    onClose: () => void;
  };
  const externalSockets = new Map<string, ExternalSocket>();
  let nextWsId = 1;
  // catalog -> wsId, for detaching/reconnecting external catalogs.
  const externalBindings = new Map<string, string>();
  // catalog -> its session ns on the shared socket.
  const catalogNs = new Map<string, number>();
  // wsId -> catalogs attached on it; refcount so DETACH tears down only on the last catalog.
  const wsIdToCatalogs = new Map<string, Set<string>>();

  // Inbound demux lives in ./virtual-socket/ws-demux (deliverInbound + bindPump).
  const catalogs = new Map<string, CatalogEntry>();
  function writeCatalogs() {
    encodeCatalogData(catalogs, catalogLenView, catalogRegion, catalogEncoder);
  }

  // These helper workers do their work over SAB/Atomics, not the event loop. Under Bun a
  // classic worker that parks on worker_threads' parentPort never releases the parent loop on
  // terminate(), so unref them at spawn; the duckdb worker stays ref'd and db.terminate() frees it.
  const unref = (w: Worker) =>
    (w as unknown as { unref?: () => void }).unref?.();

  const fetchWorker = new Worker(fetchWorkerUrl, { type: "classic" });
  unref(fetchWorker);
  fetchWorker.addEventListener("error", function (e) {
    log.error("fetch-worker error:", e.message || e);
  });
  fetchWorker.postMessage({ type: "init", sab, debug });

  // Private channel for n6k control frames; keeps them off duckdb-wasm's RPC postMessage binding.
  const n6kChannel = new MessageChannel();

  const worker = new Worker(duckdbWorkerUrl, { type: "classic" });
  worker.addEventListener("error", function (e) {
    log.error("duckdb-worker error:", e.message || e);
    log.error(
      "duckdb-worker error detail — filename:",
      e.filename,
      "lineno:",
      e.lineno,
    );
  });
  worker.postMessage(
    { type: "n6k-init", sab, mainWorkerUrl, n6kPort: n6kChannel.port2, debug },
    [n6kChannel.port2],
  );

  // ws-workers torn down on purpose; their "closed" message must not report as "disconnected".
  const intentionalClose = new WeakSet<Worker>();

  // One ws-worker per catalog; dispose = graceful WS close then terminate backstop (avoids half-open server conns).
  const wsWorkers = createCatalogRegistry<Worker>(undefined, function (w) {
    intentionalClose.add(w);
    try {
      w.postMessage({ type: "close" });
    } catch {
      /* ignore */
    }
    setTimeout(function () {
      try {
        w.terminate();
      } catch {
        /* ignore */
      }
    }, 100);
  });

  function spawnWsWorker(catalog: string, sab: SharedArrayBuffer): Worker {
    const w = new Worker(wsWorkerUrl, { type: "classic" });
    unref(w);
    w.addEventListener("error", function (ev) {
      log.error("ws-worker error:", ev.message || ev);
    });
    w.postMessage({ type: "init-sab", sab, catalog, debug });
    return w;
  }

  // Byte-pump ws-worker: moves frames WS↔channel; no atomics handshake (protocol is in C++ WsClient).
  function spawnPumpWsWorker(catalog: string, carrier: ChannelCarrier): Worker {
    const prev = wsWorkers.get(catalog);
    if (prev) {
      intentionalClose.add(prev);
      prev.terminate();
    }
    const w = new Worker(wsWorkerUrl, { type: "classic" });
    unref(w);
    w.addEventListener("error", function (ev) {
      log.error("ws-worker (pump) error:", ev.message || ev);
    });
    w.postMessage({ type: "init-channel", catalog, debug, ...carrier });
    wsWorkers.set(catalog, w);

    let connected = false;
    let disconnectNotified = false;
    w.addEventListener("message", function (ev: MessageEvent) {
      if (!ev.data) return;
      if (ev.data.type === "pump-ring-released") {
        // The pump finished with its ring; free the heap region iff a detach is pending
        // for this catalog (a plain socket drop leaves it for the reconnect to reuse).
        const off = pendingRingFree.get(catalog);
        if (off !== undefined) {
          pendingRingFree.delete(catalog);
          try {
            worker.postMessage({
              type: "n6k-vsock-free-ring",
              catalog,
              ringOffset: off,
            });
          } catch {
            // duckdb worker already torn down; region goes with the whole heap.
          }
        }
        return;
      }
      if (ev.data.type === "ready") {
        connected = true;
        notifyStatus(catalog, "connected");
      } else if (
        (ev.data.type === "closed" || ev.data.type === "error") &&
        connected &&
        !disconnectNotified &&
        !intentionalClose.has(w)
      ) {
        disconnectNotified = true;
        log.error(
          `ws-worker (pump) disconnected: catalog=${catalog} reason=${ev.data.reason || "(none)"}`,
        );
        notifyStatus(catalog, "disconnected");
      }
    } as EventListener);
    return w;
  }

  // Respawn ws-worker over the same ws-SAB and wire its lifecycle; signalAttachHandshake fires only on initial attach.
  function spawnAndWireWsWorker(
    catalog: string,
    sab: SharedArrayBuffer,
    signalAttachHandshake: boolean,
  ): Worker {
    // Terminate the prior worker synchronously: a single-waiter notify could wake the dead one and lose the request.
    const prev = wsWorkers.get(catalog);
    if (prev) {
      intentionalClose.add(prev);
      prev.terminate();
    }
    const w = spawnWsWorker(catalog, sab);
    wsWorkers.set(catalog, w);

    // Pre-handshake close = connect failure ("error"); post-handshake close = mid-session drop ("disconnected").
    let connected = false;
    let disconnectNotified = false;
    // Persistent listener: forward PUSH events and surface mid-session drops (separate from readyHandler).
    w.addEventListener("message", function (ev: MessageEvent) {
      if (!ev.data) return;
      if (ev.data.type === "push") {
        try {
          worker.postMessage({
            type: "n6k-push",
            catalog: ev.data.catalog,
            op: ev.data.op,
            body: ev.data.body,
          });
        } catch {
          // duckdb-worker already torn down (detach/teardown): drop the push.
        }
      } else if (
        (ev.data.type === "closed" || ev.data.type === "error") &&
        connected &&
        !disconnectNotified &&
        !intentionalClose.has(w)
      ) {
        // Socket dropped after handshake; surface once (close+error can both fire). No handshake re-signal.
        disconnectNotified = true;
        log.error(
          `ws-worker disconnected: catalog=${catalog} reason=${ev.data.reason || "(none)"}`,
        );
        notifyStatus(catalog, "disconnected");
      }
    } as EventListener);

    const readyHandler = function (ev: MessageEvent) {
      if (!ev.data) return;
      if (ev.data.type === "ready") {
        w.removeEventListener("message", readyHandler as EventListener);
        log.debug(`ws-worker ready: catalog=${catalog}`, ev.data);
        connected = true;
        notifyStatus(catalog, "connected");
        if (signalAttachHandshake) {
          signalDuckDBWorkerConnectionSucceeded(control);
        }
      } else if (ev.data.type === "closed" || ev.data.type === "error") {
        w.removeEventListener("message", readyHandler as EventListener);
        log.error(
          `ws-worker connect failed (closed/errored before HELLO_ACK): ` +
            `catalog=${catalog} reason=${ev.data.reason || ev.data.message || "(none)"}`,
        );
        notifyStatus(catalog, "error");
        if (signalAttachHandshake) {
          signalDuckDBWorkerConnectionFailed(control);
        }
      }
    };
    w.addEventListener("message", readyHandler as EventListener);
    return w;
  }

  // Internal path: the driver opens and owns the WebSocket.
  function connectCatalog(
    catalog: string,
    wsUrl: string,
    token: string,
    sab: SharedArrayBuffer,
    signalAttachHandshake: boolean,
  ): void {
    const w = spawnAndWireWsWorker(catalog, sab, signalAttachHandshake);
    log.debug(`ws connect attempt (internal): catalog=${catalog} url=${wsUrl}`);
    w.postMessage({ type: "open", url: wsUrl, token });
  }

  // External path: bridge an app-registered socket (by wsId) to this catalog's ws-worker.
  function connectCatalogExternal(
    catalog: string,
    wsId: string,
    token: string,
    sab: SharedArrayBuffer,
    signalAttachHandshake: boolean,
  ): void {
    const state = externalSockets.get(wsId);
    if (!state) {
      log.error(
        "attach-ws: unknown wsId (call registerWebsocket first):",
        wsId,
      );
      notifyStatus(catalog, "error");
      if (signalAttachHandshake) {
        signalDuckDBWorkerConnectionFailed(control);
      }
      return;
    }

    const w = spawnAndWireWsWorker(catalog, sab, signalAttachHandshake);

    // Bind inbound frames to the new ws-worker (replaying buffered); legacy single-session path uses ns 0.
    bindPump(state, 0, w);
    externalBindings.set(catalog, wsId);
    catalogNs.set(catalog, 0);
    let cats = wsIdToCatalogs.get(wsId);
    if (!cats) {
      cats = new Set();
      wsIdToCatalogs.set(wsId, cats);
    }
    cats.add(catalog);

    // ws-worker -> app socket: send outbound n6k frames over the shared socket.
    w.addEventListener("message", function (ev: MessageEvent) {
      if (ev.data && ev.data.type === "ws-out") {
        try {
          state.socket.send(ev.data.data as ArrayBuffer);
        } catch (error) {
          log.error("external ws send failed:", error);
        }
      }
    } as EventListener);

    // Defer open until the socket is connected (sending on a CONNECTING socket throws).
    const openTransport = () => w.postMessage({ type: "open-external", token });
    log.debug(
      `ws connect attempt (external): catalog=${catalog} wsId=${wsId} ` +
        `socketReadyState=${state.socket.readyState}`,
    );
    if (state.socket.readyState === WebSocket.OPEN) {
      openTransport();
    } else {
      state.socket.addEventListener("open", openTransport, { once: true });
    }
  }

  // Attach the durable inbound bridge to an app socket under wsId, replacing any prior binding.
  function bindExternalSocket(wsId: string, socket: WebSocket): void {
    const prev = externalSockets.get(wsId);
    if (prev) teardownExternalSocket(prev);
    try {
      socket.binaryType = "arraybuffer";
    } catch {
      // Some environments lock binaryType; fine if already "arraybuffer".
    }
    const state: ExternalSocket = {
      socket,
      inbound: [],
      targets: new Map(),
      onMessage: () => {},
      onClose: () => {},
    };
    // Binary-only, non-destructive: demux each frame to its pump; text belongs to the app.
    state.onMessage = function (ev: MessageEvent) {
      const d = ev.data;
      if (!(d instanceof ArrayBuffer)) return;
      deliverInbound(state, d);
    };
    // Socket drop -> notify every bound pump; the driver never closes the app's socket.
    state.onClose = function () {
      for (const w of state.targets.values()) {
        try {
          w.postMessage({ type: "ws-closed", reason: "app socket closed" });
        } catch {
          // worker already terminated (detach) — nothing to notify.
        }
      }
    };
    socket.addEventListener("message", state.onMessage as EventListener);
    socket.addEventListener("close", state.onClose);
    socket.addEventListener("error", state.onClose);
    externalSockets.set(wsId, state);
  }

  // Remove our listeners from an app socket without closing it — the app owns it.
  function teardownExternalSocket(state: ExternalSocket): void {
    state.targets.clear();
    state.socket.removeEventListener(
      "message",
      state.onMessage as EventListener,
    );
    state.socket.removeEventListener("close", state.onClose);
    state.socket.removeEventListener("error", state.onClose);
  }

  // Forget one catalog's binding on DETACH; only the LAST catalog on a wsId tears down the socket registration.
  function detachExternalBinding(catalog: string): void {
    const wsId = externalBindings.get(catalog);
    if (wsId === undefined) return;
    externalBindings.delete(catalog);
    const state = externalSockets.get(wsId);
    const ns = catalogNs.get(catalog);
    if (state !== undefined && ns !== undefined) state.targets.delete(ns);
    catalogNs.delete(catalog);
    const cats = wsIdToCatalogs.get(wsId);
    if (cats) {
      cats.delete(catalog);
      if (cats.size > 0) {
        // Siblings still ride this socket; ask the server to close just THIS session.
        if (state !== undefined && ns !== undefined && ns !== 0) {
          sendCloseSession(state.socket, ns);
        }
        return;
      }
      wsIdToCatalogs.delete(wsId);
    }
    if (state) {
      teardownExternalSocket(state);
      externalSockets.delete(wsId);
    }
  }

  // Close one mux session (CANCEL with reserved req id 0) on a socket that stays open for siblings.
  function sendCloseSession(socket: WebSocket, ns: number): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(packFrame({ t: FT.CANCEL, id: 0, ns }));
    } catch (error) {
      log.error("failed to send close-session frame:", error);
    }
  }

  function registerWebsocket(socket: WebSocket): string {
    const id = `ext_${nextWsId++}`;
    bindExternalSocket(id, socket);
    return id;
  }

  function replaceWebsocket(wsId: string, socket: WebSocket): void {
    // Snapshot catalogs before rebinding (bindExternalSocket clears the prior socket's targets).
    const cats = [...(wsIdToCatalogs.get(wsId) ?? [])];
    bindExternalSocket(wsId, socket);
    if (cats.length === 0) return; // registered but not attached yet — swap is enough.
    for (const catalog of cats) {
      // Byte-pump external catalog: reconnect() resets the channel and respawns the pump.
      if (pumpParams.get(catalog)?.external) {
        reconnect(catalog);
        continue;
      }
      const params = connectParams.get(catalog);
      if (!params || !params.external) {
        log.error(
          "replaceWebsocket: catalog is not backed by a registered socket:",
          catalog,
        );
        continue;
      }
      notifyStatus(catalog, "reconnecting");
      connectCatalogExternal(catalog, wsId, params.token, params.sab, false);
    }
  }

  // Reopen a dropped catalog's socket, reusing stored params + ws-SAB; the catalog stays attached.
  function reconnect(catalog: string): void {
    // Byte-pump catalog: reset channel rings and respawn the pump; WsClient re-runs HELLO on next request.
    const pp = pumpParams.get(catalog);
    if (pp) {
      notifyStatus(catalog, "reconnecting");
      const carrier = carrierOf(pp);
      resetCarrier(carrier);
      const w = spawnPumpWsWorker(catalog, carrier);
      if (pp.external) {
        const state = externalSockets.get(pp.wsId);
        if (state) {
          bindPump(state, catalogNs.get(catalog) ?? 0, w);
          w.addEventListener("message", function (ev: MessageEvent) {
            if (ev.data && ev.data.type === "ws-out") {
              try {
                state.socket.send(ev.data.data as ArrayBuffer);
              } catch (error) {
                log.error("external vsock send failed:", error);
              }
            }
          } as EventListener);
          const openTransport = () =>
            w.postMessage({ type: "open-pump-external" });
          if (state.socket.readyState === WebSocket.OPEN) openTransport();
          else
            state.socket.addEventListener("open", openTransport, {
              once: true,
            });
        }
      } else {
        w.postMessage({ type: "open-pump", url: pp.wsUrl });
      }
      return;
    }

    const params = connectParams.get(catalog);
    if (!params) {
      log.error("reconnect: unknown catalog (not attached):", catalog);
      return;
    }
    notifyStatus(catalog, "reconnecting");
    if (params.external) {
      // The driver can't reopen a socket it doesn't own; rebind to whatever is registered for this wsId.
      connectCatalogExternal(
        catalog,
        params.wsId,
        params.token,
        params.sab,
        false,
      );
    } else {
      connectCatalog(catalog, params.wsUrl, params.token, params.sab, false);
    }
  }

  // Use onmessage (not addEventListener): it auto-starts the port; an unstarted port drops frames.
  // eslint-disable-next-line unicorn/prefer-add-event-listener
  n6kChannel.port1.onmessage = function (e) {
    if (!e.data) return;

    switch (e.data.type) {
      case "n6k-attach": {
        const baseUrl: string = e.data.baseUrl;
        const wsUrl: string = e.data.wsUrl;
        const token: string = e.data.token || "";
        const catalog: string = e.data.catalog || "";
        const sab: SharedArrayBuffer = e.data.wsSab;

        // Keep the token out of the WS URL (leaks into logs); it rides an FT_HELLO frame instead.
        catalogs.set(baseUrl, { t: token });
        writeCatalogs();

        connectParams.set(catalog, { external: false, wsUrl, token, sab });
        connectCatalog(catalog, wsUrl, token, sab, true);
        break;
      }
      case "n6k-attach-ws": {
        // Attach onto an app-registered socket owned by the page.
        const wsId: string = e.data.wsId || "";
        const token: string = e.data.token || "";
        const catalog: string = e.data.catalog || "";
        const sab: SharedArrayBuffer = e.data.wsSab;

        const state = externalSockets.get(wsId);
        if (state && token) {
          try {
            const u = new URL(state.socket.url);
            const httpProto = u.protocol === "wss:" ? "https:" : "http:";
            catalogs.set(`${httpProto}//${u.host}`, { t: token });
            writeCatalogs();
          } catch {
            // Non-absolute socket.url (rare): skip the HTTP token mapping.
          }
        }

        connectParams.set(catalog, { external: true, wsId, token, sab });
        connectCatalogExternal(catalog, wsId, token, sab, true);
        break;
      }
      case "n6k-detach": {
        const catalog: string | undefined = e.data.catalog;
        if (catalog) {
          detachExternalBinding(catalog);
          wsWorkers.remove(catalog);
          connectParams.delete(catalog);
        } else {
          for (const c of externalBindings.keys()) detachExternalBinding(c);
          wsIdToCatalogs.clear();
          catalogNs.clear();
          wsWorkers.clear();
          connectParams.clear();
        }
        break;
      }
      case "n6k-vsock-attach": {
        // Byte-pump path: spawn a pump ws-worker that dials wsUrl and moves frames WS↔channel.
        const catalog: string = e.data.catalog || "";
        const wsUrl: string = e.data.wsUrl || "";
        const carrier = readCarrier(e.data);
        pumpParams.set(catalog, { external: false, wsUrl, ...carrier });
        const w = spawnPumpWsWorker(catalog, carrier);
        w.postMessage({ type: "open-pump", url: wsUrl });
        break;
      }
      case "n6k-vsock-attach-ws": {
        // Byte-pump over an app-registered socket: main thread bridges binary frames to/from the pump.
        const catalog: string = e.data.catalog || "";
        const wsId: string = e.data.wsId || "";
        // Session ns (from C++ WsClient, echoed by server) for demux; defaults to 0 for pre-ns clients.
        const ns: number = typeof e.data.ns === "number" ? e.data.ns : 0;
        const carrier = readCarrier(e.data);
        const state = externalSockets.get(wsId);
        if (!state) {
          log.error("n6k-vsock-attach-ws: unknown wsId:", wsId);
          notifyStatus(catalog, "error");
          break;
        }
        const w = spawnPumpWsWorker(catalog, carrier);
        // Route this session's inbound frames (by ns) to its pump, replaying any buffered.
        bindPump(state, ns, w);
        externalBindings.set(catalog, wsId);
        catalogNs.set(catalog, ns);
        let cats = wsIdToCatalogs.get(wsId);
        if (!cats) {
          cats = new Set();
          wsIdToCatalogs.set(wsId, cats);
        }
        cats.add(catalog);
        pumpParams.set(catalog, { external: true, wsId, ...carrier });
        // pump worker → app socket: relay outbound frames over the shared socket.
        w.addEventListener("message", function (ev: MessageEvent) {
          if (ev.data && ev.data.type === "ws-out") {
            try {
              state.socket.send(ev.data.data as ArrayBuffer);
            } catch (error) {
              log.error("external vsock send failed:", error);
            }
          }
        } as EventListener);
        const openTransport = () =>
          w.postMessage({ type: "open-pump-external" });
        if (state.socket.readyState === WebSocket.OPEN) {
          openTransport();
        } else {
          state.socket.addEventListener("open", openTransport, { once: true });
        }
        break;
      }
      case "n6k-vsock-detach": {
        const catalog: string | undefined = e.data.catalog;
        if (catalog) {
          // Heap-backed: schedule the ring-region free, honored once the pump acks release
          // (pump-ring-released). Set BEFORE closing the pump so the ack isn't missed.
          const pp = pumpParams.get(catalog);
          if (pp && "wasmMemory" in pp) {
            pendingRingFree.set(catalog, pp.ringOffset);
          }
          detachExternalBinding(catalog);
          wsWorkers.remove(catalog);
          pumpParams.delete(catalog);
        }
        break;
      }
      // No default
    }
  };

  function dispose(): void {
    wsWorkers.clear(); // graceful WS close + terminate backstop per ws-worker
    try {
      fetchWorker.terminate();
    } catch {
      /* ignore */
    }
    try {
      n6kChannel.port1.close();
    } catch {
      /* ignore */
    }
  }

  return { worker, reconnect, registerWebsocket, replaceWebsocket, dispose };
}
