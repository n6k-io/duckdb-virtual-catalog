/**
 * Bun WASM DuckDB REPL with n6k extension support.
 *
 *   cd packages/npm && bun run scripts/wasm-repl.ts
 *   cd packages/npm && bun run scripts/wasm-repl.ts --debug
 *   cd packages/npm && bun run scripts/wasm-repl.ts --repo bucket
 *   cd packages/npm && bun run scripts/wasm-repl.ts --repo https://storage.googleapis.com/n6k-duckdb-release/test
 *
 * --repo <local|bucket|URL>
 *   local  (default) serve packages/npm/wasm over a local Bun.serve
 *   bucket            use https://storage.googleapis.com/n6k-duckdb-release
 *   <URL>             use the given URL as custom_extension_repository
 */
import * as duckdb from "@duckdb/duckdb-wasm";
import path from "path";
import readline from "readline";
import { parseArgs } from "util";
import { createN6kWorker } from "../src/create-n6k-worker";

const { values: args } = parseArgs({
  options: {
    "no-n6k": { type: "boolean", default: false },
    debug: { type: "boolean", default: false },
    repo: { type: "string", default: "local" },
    c: { type: "string" },
  },
});
const useN6k = !args["no-n6k"];

const BUCKET_REPO = "https://storage.googleapis.com/n6k-duckdb-release";
const repoArg = args.repo ?? "local";
const repoMode: "local" | "remote" =
  repoArg === "local" ? "local" : "remote";
const remoteRepoUrl =
  repoArg === "bucket"
    ? BUCKET_REPO
    : repoArg === "local"
      ? null
      : repoArg;

const DIST = path.resolve(
  import.meta.dir,
  "../node_modules/@duckdb/duckdb-wasm/dist",
);
// Use the EH (native wasm exception handling) variant: the MVP worker in
// @duckdb/duckdb-wasm@1.33.1-dev57.0 has a `_setThrew is not defined` bug on
// any exception unwind through invoke_*.
const WASM = path.join(DIST, "duckdb-eh.wasm");
const DUCKDB_WORKER = path.join(DIST, "duckdb-node-eh.worker.cjs");

let worker: any;

if (useN6k) {
  worker = createN6kWorker({
    mainWorkerUrl: DUCKDB_WORKER,
    fetchWorkerUrl: path.join(import.meta.dir, "n6k-fetch-worker.ts"),
    duckdbWorkerUrl: path.join(import.meta.dir, "n6k-duckdb-worker.ts"),
    onStatus: (status) => console.error(`[n6k] status: ${status}`),
  });
} else {
  worker = new Worker(DUCKDB_WORKER);
}

const logger = args.debug ? new duckdb.ConsoleLogger() : new duckdb.VoidLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);

process.stdout.write("Loading WASM...\n");
const wasmPkg = require("@duckdb/duckdb-wasm/package.json");
process.stdout.write(`  @duckdb/duckdb-wasm: ${wasmPkg.version}\n`);
process.stdout.write(`  wasm binary: ${WASM}\n`);
await db.instantiate(WASM);

const openConfig: any = {
  accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
};
if (useN6k) {
  openConfig.allowUnsignedExtensions = true;
}

await db.open(openConfig);
const conn = await db.connect();

const versionResult = await conn.query("SELECT version() AS v");
const duckdbVersion = versionResult.toArray()[0]?.toJSON().v ?? "unknown";
process.stdout.write(`  duckdb runtime: ${duckdbVersion}\n`);

if (useN6k) {
  let extRepo: string;
  if (repoMode === "local") {
    const wasmDir = path.resolve(import.meta.dir, "../wasm");
    const extServer = Bun.serve({
      port: 0,
      async fetch(req) {
        const url = new URL(req.url);
        const filePath = path.join(wasmDir, url.pathname);
        const file = Bun.file(filePath);
        if (await file.exists()) {
          return new Response(file);
        }
        return new Response("not found", { status: 404 });
      },
    });
    extRepo = `http://localhost:${extServer.port}`;
    process.stdout.write(
      `  extension repo: :${extServer.port} (${wasmDir})\n`,
    );
  } else {
    extRepo = remoteRepoUrl!;
    process.stdout.write(`  extension repo: ${extRepo}\n`);
  }
  await conn.query(`SET custom_extension_repository = '${extRepo}';`);
  await conn.query("LOAD n6k_client;");
  process.stdout.write("DuckDB WASM ready (n6k mode).\n");
} else {
  process.stdout.write("DuckDB WASM ready.\n");
}

if (args.c !== undefined) {
  try {
    const result = await conn.query(args.c);
    const rows = result.toArray().map((row: any) => row.toJSON());
    if (rows.length === 0) {
      process.stdout.write("OK\n");
    } else {
      console.table(rows);
    }
    await conn.close();
    await db.terminate();
    process.exit(0);
  } catch (e: any) {
    process.stderr.write(`Error: ${e.message}\n`);
    await conn.close();
    await db.terminate();
    process.exit(1);
  }
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: "D ",
});

let buffer = "";

rl.prompt();
rl.on("line", async (line: string) => {
  const trimmed = line.trim();
  if (trimmed === ".exit") {
    await conn.close();
    await db.terminate();
    process.exit(0);
  }

  buffer += (buffer ? " " : "") + line;
  if (!buffer.trimEnd().endsWith(";")) {
    process.stdout.write("  ");
    return;
  }

  const sql = buffer;
  buffer = "";

  try {
    const result = await conn.query(sql);
    const rows = result.toArray().map((row: any) => row.toJSON());
    if (rows.length === 0) {
      process.stdout.write("OK\n");
    } else {
      console.table(rows);
    }
  } catch (e: any) {
    process.stderr.write(`Error: ${e.message}\n`);
  }

  rl.prompt();
});

rl.on("close", async () => {
  await conn.close();
  await db.terminate();
  process.exit(0);
});
