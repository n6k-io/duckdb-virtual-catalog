// duckdb-worker half of the vsock: the n6k.vsock* functions the wasm C++ calls, backed by the duplex channel.
// recv return protocol (matches ws_reactor_wasm.cpp DrainInbound):
//   >= 0  frame of that length copied into HEAP@ptr (consumed)
//   -1    RX ring empty
//   -3    RX ring empty AND closed — fail fast
//   < -3  next frame larger than cap; length is (-n), nothing consumed

import { duckdbEndpoint, type ChannelLayout, type Endpoint } from "./channel";

export type VsockRegistry = {
  // Register the duckdb-side endpoint for a catalog's channel (on attach). `buf` is
  // either a standalone SharedArrayBuffer (bootstrap) or duckdb's shared wasm heap;
  // `baseOffset` locates the channel within it (the C++ malloc pointer, 0 otherwise).
  register(
    catalog: string,
    buf: ArrayBufferLike,
    layout?: ChannelLayout,
    baseOffset?: number,
  ): void;
  // Drop and close a catalog's channel (on detach).
  unregister(catalog: string): void;
  // n6k.vsockSend: write one frame to the TX ring; blocking so a full ring applies backpressure.
  send(catalog: string, ptr: number, len: number, heap: Uint8Array): void;
  // n6k.vsockRecv: copy the next RX frame into HEAP@ptr. See return protocol above.
  recv(catalog: string, ptr: number, cap: number, heap: Uint8Array): number;
  // n6k.vsockWait: block up to timeoutMs for the next RX frame (worker thread).
  wait(catalog: string, timeoutMs: number): void;
  // n6k.vsockEpoch: reset counter so the C++ client detects a reset and re-HELLOs; -1 if unregistered.
  epoch(catalog: string): number;
  // n6k.vsockClose: close the channel (from the reactor's Stop).
  close(catalog: string): void;
};

export function createVsockRegistry(): VsockRegistry {
  const channels = new Map<string, Endpoint>();

  return {
    register(catalog, buf, layout, baseOffset = 0) {
      channels.set(catalog, duckdbEndpoint(buf, layout, baseOffset));
    },

    unregister(catalog) {
      const ep = channels.get(catalog);
      if (ep) ep.close();
      channels.delete(catalog);
    },

    send(catalog, ptr, len, heap) {
      const ep = channels.get(catalog);
      if (!ep) return;
      // Copy out of the heap: the frame must own its bytes; C++ frees its staging buffer on return.
      ep.sendBlocking(heap.slice(ptr, ptr + len));
    },

    recv(catalog, ptr, cap, heap) {
      const ep = channels.get(catalog);
      if (!ep) return -3; // gone — treat as closed so the reactor fails fast
      const len = ep.peekLen();
      if (len === -1) return ep.inboundClosed() ? -3 : -1;
      if (len > cap) return -len; // too big for the caller's buffer; don't consume
      const frame = ep.tryRecv();
      if (frame === null) return ep.inboundClosed() ? -3 : -1;
      heap.set(frame, ptr);
      return frame.byteLength;
    },

    wait(catalog, timeoutMs) {
      const ep = channels.get(catalog);
      if (!ep) return;
      ep.recvWaitTimed(timeoutMs);
    },

    epoch(catalog) {
      const ep = channels.get(catalog);
      return ep ? ep.epoch() : -1;
    },

    close(catalog) {
      const ep = channels.get(catalog);
      if (ep) ep.close();
    },
  };
}
