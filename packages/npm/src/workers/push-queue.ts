// Per-catalog queue of server PUSH events; single-threaded JS, no locking.

export interface PushEvent {
  op: number;
  body: unknown;
}

export interface PushQueue {
  enqueue(catalog: string, op: number, body: unknown): void;
  take(catalog: string): string;
}

export function createPushQueue(): PushQueue {
  const queues = new Map<string, PushEvent[]>();
  return {
    enqueue(catalog: string, op: number, body: unknown): void {
      const q = queues.get(catalog);
      if (q) {
        q.push({ op, body });
      } else {
        queues.set(catalog, [{ op, body }]);
      }
    },
    take(catalog: string): string {
      const q = queues.get(catalog);
      if (!q || q.length === 0) return "";
      queues.set(catalog, []);
      // JSON-stringify each body so the C++ caller sees the same shape the native PUSH path delivers.
      return JSON.stringify(
        q.map((e) => ({ op: e.op, body: JSON.stringify(e.body) })),
      );
    },
  };
}
