import { test, expect, describe } from "bun:test";
import { wsWorkerEndpoint, channelBytes, type ChannelLayout } from "./channel";
import { createVsockRegistry } from "./duckdb-glue";

const LAYOUT: ChannelLayout = {
  duckdbToWsCapacity: 1024,
  wsToDuckdbCapacity: 1024,
};

function setup() {
  const sab = new SharedArrayBuffer(channelBytes(LAYOUT));
  const reg = createVsockRegistry();
  reg.register("cat", sab, LAYOUT);
  const ws = wsWorkerEndpoint(sab, LAYOUT);
  const heap = new Uint8Array(1 << 16);
  return { reg, ws, heap };
}

function bytes(...v: number[]): Uint8Array {
  return Uint8Array.from(v);
}

describe("duckdb vsock glue", () => {
  test("send: C++ frame reaches the ws side intact", () => {
    const { reg, ws, heap } = setup();
    heap.set(bytes(1, 2, 3, 4), 100);
    reg.send("cat", 100, 4, heap);
    const got = ws.tryRecv();
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([1, 2, 3, 4]);
  });

  test("recv: -1 when empty", () => {
    const { reg, heap } = setup();
    expect(reg.recv("cat", 0, heap.byteLength, heap)).toBe(-1);
  });

  test("recv: copies a frame into HEAP@ptr and returns its length", () => {
    const { reg, ws, heap } = setup();
    ws.send(bytes(9, 8, 7));
    const n = reg.recv("cat", 200, heap.byteLength, heap);
    expect(n).toBe(3);
    expect([...heap.subarray(200, 203)]).toEqual([9, 8, 7]);
  });

  test("recv: oversized frame returns -len and does NOT consume", () => {
    const { reg, ws, heap } = setup();
    ws.send(bytes(1, 2, 3, 4, 5, 6, 7, 8));
    const n = reg.recv("cat", 0, 4, heap);
    expect(n).toBe(-8);
    const n2 = reg.recv("cat", 0, 8, heap);
    expect(n2).toBe(8);
    expect([...heap.subarray(0, 8)]).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  test("multiple frames recv in order", () => {
    const { reg, ws, heap } = setup();
    ws.send(bytes(10));
    ws.send(bytes(20, 21));
    expect(reg.recv("cat", 0, 64, heap)).toBe(1);
    expect(heap[0]).toBe(10);
    expect(reg.recv("cat", 0, 64, heap)).toBe(2);
    expect([...heap.subarray(0, 2)]).toEqual([20, 21]);
    expect(reg.recv("cat", 0, 64, heap)).toBe(-1);
  });

  test("unknown catalog recv returns -3 (closed) so the reactor fails fast", () => {
    const { reg, heap } = setup();
    expect(reg.recv("nope", 0, 64, heap)).toBe(-3);
    expect(() => reg.send("nope", 0, 0, heap)).not.toThrow();
    expect(() => reg.wait("nope", 0)).not.toThrow();
    expect(() => reg.close("nope")).not.toThrow();
  });

  test("unregister closes and forgets the channel (recv -3)", () => {
    const { reg, ws, heap } = setup();
    ws.send(bytes(5));
    reg.unregister("cat");
    expect(reg.recv("cat", 0, 64, heap)).toBe(-3);
  });

  test("a dropped channel (ws side closed) recv returns -3", () => {
    const { reg, ws, heap } = setup();
    ws.close();
    expect(reg.recv("cat", 0, 64, heap)).toBe(-3);
  });

  test("queued frames still drain before -3 after close", () => {
    const { reg, ws, heap } = setup();
    ws.send(bytes(7, 7));
    ws.close();
    expect(reg.recv("cat", 0, 64, heap)).toBe(2);
    expect([...heap.subarray(0, 2)]).toEqual([7, 7]);
    expect(reg.recv("cat", 0, 64, heap)).toBe(-3);
  });
});

// Phase 1: the channel lives inside duckdb's shared wasm heap at a malloc offset, and
// the send/recv staging pointers index that same heap — the realistic coi topology.
describe("duckdb vsock glue over a heap-backed offset region", () => {
  const OFFSET = 512; // 4-aligned malloc-ptr stand-in, clear of the staging pointers

  function heapSetup() {
    const mem = new WebAssembly.Memory({
      initial: 1,
      maximum: 4,
      shared: true,
    });
    const reg = createVsockRegistry();
    reg.register("cat", mem.buffer, LAYOUT, OFFSET);
    const ws = wsWorkerEndpoint(mem.buffer, LAYOUT, OFFSET);
    // `heap` is the whole wasm memory, exactly as HEAPU8 is in the worker; staging
    // pointers 100/200 sit below OFFSET so they never overlap the ring region.
    const heap = new Uint8Array(mem.buffer);
    return { reg, ws, heap };
  }

  test("send: a C++ frame staged in the heap reaches the ws side intact", () => {
    const { reg, ws, heap } = heapSetup();
    heap.set(bytes(1, 2, 3, 4), 100);
    reg.send("cat", 100, 4, heap);
    expect([...ws.tryRecv()!]).toEqual([1, 2, 3, 4]);
  });

  test("recv: a ws frame is copied back into the heap at ptr", () => {
    const { reg, ws, heap } = heapSetup();
    ws.send(bytes(9, 8, 7));
    const n = reg.recv("cat", 200, 64, heap);
    expect(n).toBe(3);
    expect([...heap.subarray(200, 203)]).toEqual([9, 8, 7]);
  });
});
