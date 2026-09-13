/**
 * Bun-compatible n6k DuckDB worker.
 *
 * Shims importScripts → require, then loads the n6k duckdb-worker
 * which sets up globalThis.n6k and calls importScripts(mainWorkerUrl)
 * to load the actual duckdb-wasm worker code.
 */
(globalThis as any).importScripts = (...urls: string[]) => {
  for (const url of urls) require(url);
};

import "../src/workers/duckdb-worker";
