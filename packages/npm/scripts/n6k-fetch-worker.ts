/**
 * Bun-compatible n6k fetch worker.
 *
 * Shims globalThis.addEventListener/postMessage for worker_threads,
 * then loads the fetch-worker which uses browser-style worker APIs.
 */
import { parentPort } from "worker_threads";

if (parentPort) {
  const listeners = new Map<string, Function[]>();

  globalThis.addEventListener = ((type: string, fn: Function) => {
    const list = listeners.get(type) || [];
    list.push(fn);
    listeners.set(type, list);
  }) as any;

  globalThis.postMessage = ((data: any, transfer?: any) => {
    parentPort!.postMessage(data, transfer);
  }) as any;

  parentPort.on("message", (data: any) => {
    const fns = listeners.get("message") || [];
    for (const fn of fns) {
      fn({ data } as MessageEvent);
    }
  });
}

import "../src/workers/fetch-worker";
