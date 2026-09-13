# @n6k.io/db

Lightweight DuckDB WASM driver with synchronous fetch via SharedArrayBuffer workers.

Wraps `@duckdb/duckdb-wasm` workers so that DuckDB extensions can make synchronous HTTP requests from within WASM — bridging the gap between WASM's synchronous execution model and the browser's async `fetch()` API.

## Install

```bash
npm install @n6k.io/db @duckdb/duckdb-wasm
```

## Requirements

- `SharedArrayBuffer` support — your server **must** set these headers:
  ```
  Cross-Origin-Opener-Policy: same-origin
  Cross-Origin-Embedder-Policy: require-corp
  ```
- Node >= 18 (for the `@n6k.io/db/node` export)

## Usage

### Browser

```js
import * as duckdb from '@duckdb/duckdb-wasm'
import { createN6kWorker } from '@n6k.io/db'

const BUNDLES = duckdb.getJsDelivrBundles()
const bundle = await duckdb.selectBundle(BUNDLES)

// Create an n6k-wrapped worker instead of a plain Worker
const worker = createN6kWorker(bundle.mainWorker)

const logger = new duckdb.ConsoleLogger()
const db = new duckdb.AsyncDuckDB(logger, worker)
await db.instantiate(bundle.mainModule, bundle.pthreadWorker)

await db.open({ allowUnsignedExtensions: true })
const conn = await db.connect()

// Point DuckDB at your extension server
await conn.query(`SET custom_extension_repository = '${window.location.origin}';`)

// Load and use extensions
await conn.query('LOAD n6k_client;')
await conn.query("ATTACH 'n6k://example.com' AS remote (TYPE n6k);")
const result = await conn.query('SELECT * FROM remote.main.demo;')
const rows = result.toArray().map(r => r.toJSON())
```

`createN6kWorker(mainWorkerUrl)` is a drop-in replacement for the worker you'd normally pass to `AsyncDuckDB`. It spawns two internal workers:

1. A **fetch worker** that performs async HTTP requests
2. A **DuckDB wrapper worker** that exposes synchronous `self.n6k.fetch()` / `self.n6k.fetchBinary()` to WASM code via `SharedArrayBuffer` + `Atomics`

### Serving WASM Extensions

The package ships pre-built WASM extensions under `wasm/`. Your dev server needs to serve these files so DuckDB can load them at runtime.

Use the `@n6k.io/db/node` export to get the path to the WASM directory:

```js
import { wasmDir } from '@n6k.io/db/node'
// => absolute path to the `wasm/` directory inside the package
```

The directory structure is:

```
wasm/
  v1.5.4/
    wasm_threads/
      n6k_client.duckdb_extension.wasm
      ...
```

Only the `wasm_threads` (cross-origin-isolated / shared-memory) variant is shipped:
n6k requires cross-origin isolation, and the extension is built solely for shared
memory, so the coi bundle is always the browser runtime. (eh/mvp can't load a
shared-memory extension.)

Example middleware (framework-agnostic, works with Express/Connect-style servers):

```js
import path from 'path'
import fs from 'fs'
import { wasmDir } from '@n6k.io/db/node'

function serveExtensions(req, res, next) {
  const filePath = path.join(wasmDir, req.url)

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const mimeTypes = {
      '.wasm': 'application/wasm',
      '.json': 'application/json',
      '.js': 'text/javascript',
    }
    const ext = path.extname(filePath)
    res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream')
    res.setHeader('Access-Control-Allow-Origin', '*')
    fs.createReadStream(filePath).pipe(res)
    return
  }
  next()
}
```

## API

### `createN6kWorker(mainWorkerUrl: string): Worker`

Creates a DuckDB Web Worker with synchronous fetch capabilities. Pass the returned worker to `duckdb.AsyncDuckDB` in place of a regular `Worker`.

- **`mainWorkerUrl`** — URL to the DuckDB WASM main worker script (typically `bundle.mainWorker` from `duckdb.selectBundle()`)

### `wasmDir: string` (from `@n6k.io/db/node`)

Absolute path to the `wasm/` directory shipped with the package. Use this to configure your dev server to serve extension files.

## React hooks

Imported from `@n6k.io/db/react`. Built on `@tanstack/react-query` v5; you must wrap your app in a `QueryClientProvider` (use the `QueryProvider` shipped here for sensible defaults) and a `DuckDBProvider`.

```tsx
import { QueryProvider, DuckDBProvider } from "@n6k.io/db/react";

export function Root({ children }) {
  return (
    <QueryProvider>
      <DuckDBProvider databases={{ page_db: "n6k://api.example.com/p/123?token=…" }}>
        {children}
      </DuckDBProvider>
    </QueryProvider>
  );
}
```

### `DuckDBProvider`

Initializes a DuckDB-WASM connection, loads the `n6k` extension, then runs `ATTACH` for each entry in `databases`. The `databases` prop seeds the initial *desired* attachment set; subsequent prop changes are reconciled (catalogs added, removed, or pointed at a new connection string trigger DETACH/ATTACH). For dynamic per-component attachments, use `useAttach` (below).

Props:

- `databases?: Record<string, DatabaseSpec>` — map of catalog name → attach spec. A spec is either a bare path string (uses `TYPE n6k` by default) or `{ path, options? }`. Each becomes `ATTACH '<path>' AS <name> (<options>);`. `options` shallow-merges over the default `{ TYPE: "n6k" }`.
- `duckdbOptions?: CreateDuckDBOptions` — forwarded to the underlying `createDuckDB()`.

```tsx
<DuckDBProvider databases={{
  page_db: "n6k://api.example.com/p/123",
  pg: { path: "host=… dbname=…", options: { TYPE: "postgres" } },
  secure: { path: "n6k://api.example.com/p/456", options: { token: "…" } },
}}>…</DuckDBProvider>
```

### `useDuckDB()`

Returns the provider's context value: `{ conn, status, error, connStatus, reconnect, registerWebsocket, replaceWebsocket, desired, attached, errors, setDesired, removeDesired }`. Throws if called outside a `DuckDBProvider`. `status` transitions `"initializing" → "loading-extensions" → "ready"` (or `"error"` for conn-init / extension-load failures). `connStatus` is a per-catalog `Record<string, WsStatus>` (`"connected" | "reconnecting" | "disconnected" | "error"`), keyed by ATTACH alias. Per-catalog *attachment* status is derived from `desired`/`attached`/`errors` via `statusOf()` (see below) — `useQuery` does this automatically. `reconnect(catalog)` reopens a dropped driver-owned socket; `registerWebsocket`/`replaceWebsocket` back an attach with a socket you own (see below).

### Attaching an existing WebSocket

`registerWebsocket(socket) → wsId` and `replaceWebsocket(wsId, socket)` let you attach n6k onto a WebSocket your app already opened, instead of having the driver open its own. They are returned from `createDuckDB()` and also exposed on the `useDuckDB()` context. Browser-only.

```ts
const { conn, registerWebsocket, replaceWebsocket } = await createDuckDB();

const ws = new WebSocket("wss://api.example.com/ws?catalog=page_db");
const wsId = registerWebsocket(ws); // claims only this socket's binary frames
await conn.query(`ATTACH '' AS page_db (TYPE n6k, wsId '${wsId}')`);

// later, after your app reconnects its own socket:
replaceWebsocket(wsId, newWs); // rebinds page_db without a DETACH/ATTACH
```

The driver rides the socket and claims only its **binary** frames (n6k is all-binary), so your app keeps using the same socket for its own text traffic; it never closes a socket it doesn't own. Auth (the socket's upgrade) is yours to set. Catalog binding comes from `?catalog=` in the socket URL **or** — when you dial a bare `/ws` — from the `catalog` the driver sends in its HELLO handshake (the ATTACH alias); the server errors if the URL and handshake disagree. On a drop, the catalog's `connStatus` becomes `"disconnected"`; open a fresh socket and call `replaceWebsocket`. In a React app the same functions are on `useDuckDB()`. See [n6k-network.md](../../n6k-network.md#attaching-an-existing-websocket-wasmbrowser) for the full contract, including the server-side demux requirement for a *shared* (app + n6k) socket.

#### The `catalog` option

`catalog '<name>'` names the **server-side** catalog a session opens, decoupled from the local ATTACH alias. Without it the session opens a catalog named after the alias (the historical behavior); with it, alias and server catalog are independent — so you can attach the same server catalog under different aliases, or (below) several server catalogs over one socket.

```ts
ATTACH '' AS foo (TYPE n6k, wsId '${wsId}', catalog 'analytics_prod');
// local catalog `foo` ⇒ server catalog `analytics_prod`
```

#### Multiplexing several catalogs over one socket

One registered socket can back **several** catalog sessions at once — dial the server's multiplexing mount (which drives a `WsV2Router`) and give each ATTACH its own `catalog`:

```ts
const ws = new WebSocket("wss://api.example.com/mux/ws"); // no ?catalog=
const wsId = registerWebsocket(ws);
await conn.query(`ATTACH '' AS foo (TYPE n6k, wsId '${wsId}', catalog 'catA')`);
await conn.query(`ATTACH '' AS bar (TYPE n6k, wsId '${wsId}', catalog 'catB')`);
// foo and bar now run concurrently over the ONE socket.
await conn.query(`DETACH foo`); // bar keeps working; only foo's session closes
```

Each attach is tagged with a session id (`ns`) that the driver stamps on its frames and the server echoes back, so responses are demuxed to the right catalog; req-id spaces are per-session, so they never collide. A per-session build failure (e.g. a bad `catalog`) fails only that ATTACH — siblings and the socket are unaffected. `DETACH` of one catalog closes just its server session (the socket stays open for the others); `replaceWebsocket` rebinds **every** session riding the socket onto the new one. Requires a server mount opened with `multiplex=True` (a shared socket can't name N catalogs in one URL, so each catalog rides its own HELLO frame). Multiplexing is **browser/wsId only** — the native `wsFd` path stays one catalog per socket.

### `useQuery(sql, options?)`

Runs a SELECT and returns `{ columns, rows, schema, status, statusInfo, error }`.

- `status: "idle" | "loading" | "ready" | "error"` — coarse state.
- `statusInfo: "noQuery" | "awaitingDb" | "awaitingAttach" | "executing" | null` — finer-grained reason when `status` is `"loading"`/`"idle"`.

Options:

- `tables?: Array<string | TableRef>` — skip the auto-parse step and declare the affected tables explicitly. Strings become `(catalog: null, schema: null, table: name)` wildcards; pass a full `TableRef` for catalog-qualified entries.
- `catalogs?: Array<string | { catalog: string }>` — explicitly gate on these catalogs being `"ready"`. Use when the parser can't infer the dependency (fully unqualified table names) or to force-wait on a catalog that hasn't been declared via `useAttach` yet. An explicit entry with no provider entry yet defaults to `"pending"` (auto-discovered catalogs default to `"ready"` for native cases like `memory`/`main`).

```tsx
useQuery("select * from posts", { catalogs: ["page_db"] });
useQuery("select * from posts", { catalogs: [{ catalog: "page_db" }] });
```

The hook auto-parses your SQL to discover attachment dependencies. For `select * from foo.bar`, if `foo` is a managed catalog (declared via `databases` or `useAttach`), the query waits for it to be `"ready"`. Catalogs not in the desired set (native `memory`/`main`) don't block — use `catalogs` if you need to gate on them anyway.

The tanstack query key includes the currently-attached connection string for each managed catalog, so swapping a catalog's `connStr` automatically invalidates cached rows on the next render.

```tsx
const { rows, status, error } = useQuery("select * from page_db.public.posts");
```

### `useExec()`

Returns `{ exec, status, error }` for mutating statements.

```tsx
const { exec } = useExec();
await exec("INSERT INTO page_db.public.posts (title) VALUES ('hi')");
```

By default, the SQL is parsed for affected tables and any `useQuery` whose `tables` overlap is invalidated. Override with `exec(sql, { invalidate: [...] })` or `exec(sql, { invalidate: false })`.

### `useView(db, name, sql)`

Creates `CREATE OR REPLACE VIEW "${db}".main."${name}" AS ${sql}` and selects from it. The view is dropped when params change or the component unmounts. Same `TabularResult` shape as `useQuery` (no `statusInfo`).

### `useTableMeta(tableName)`

Returns `{ meta, status, error }` where `meta: { columns, primaryKey, isWritable, enumValues } | null`. Accepts unqualified, `schema.table`, or `catalog.schema.table`. Subquery-like inputs are skipped (`status:"idle"`).

### `useAttach(catalog, path, options?)`

Declare an ATTACH for `catalog` pointing at `path` with the given `options`. The provider's reconciler builds and runs `ATTACH '<path>' AS <catalog> (<options>)` and tracks state. `options` shallow-merges over the default `{ TYPE: "n6k" }` — pass `{ TYPE: "postgres" }` (or any other key) to override.

While the attach is in flight (or being swapped) any `useQuery` referencing `catalog` returns `status: "loading"` with `statusInfo: "awaitingAttach"`. On unmount the catalog is `DETACH`ed.

```tsx
function AppShell({ pageId, token }: { pageId: string; token: string }) {
  useAttach("page_db", `${DATA_SERVICE_URL}/${pageId}`, { token });
  return <Outlet />;
}
```

The rendered SQL for `useAttach("page_db", "https://x/123", { token: "abc" })` is:

```sql
ATTACH 'https://x/123' AS page_db (TYPE n6k, token 'abc');
```

`TYPE` is emitted unquoted (DuckDB grammar); all other option values are single-quoted with `'` escaped to `''`.

Behavior:

- **Re-attach trigger**: the reconciler compares configs structurally (`path` + `options`). Identical config = no-op. Any field different = `DETACH` + re-`ATTACH`.
- **Errors**: a failed `ATTACH` surfaces via `useQuery` as `status: "error"` with `Attach <catalog> failed: <message>`. Changing the config clears the error and retries.
- **Concurrent rapid swaps**: the reconciler reads fresh `attached` state on each pass and serializes operations; the final state matches the last config.
- **Cache invalidation**: `useQuery`'s tanstack key includes a fingerprint of the currently-attached config per managed catalog, so swaps automatically invalidate cached rows.

#### Known limitation (fixable)

There is a one-render-pass race window: when a parent re-renders with a new config, `useAttach` writes the new desired value to provider state *during render*, but React applies that update on the **next** pass. Sibling `useQuery` components rendering in the same pass still see the previous `desired` and may briefly fire against the old DB before flipping to `awaitingAttach` on the next render.

The previous `usePreflight` implementation had the same window despite comments claiming otherwise. It's fixable without changing the data model — either back `desired` with `useSyncExternalStore` (writes visible to later same-pass reads), or change the API to a `<DuckDBAttachments value={...}>` parent context component so `desired` flows top-down and children naturally see new values in the same pass. Neither is implemented yet.

### Attachment selectors

For reading per-catalog status from your own code:

```ts
import { statusOf, errorOf } from "@n6k.io/db/react";

const { desired, attached, errors } = useDuckDB();
const state = statusOf({ desired, attached, errors }, "page_db");
// => "pending" | "ready" | "error" | undefined
const err = errorOf({ desired, attached, errors }, "page_db");
// => string | undefined
```

`statusOf` returns `undefined` for catalogs that are neither desired nor attached (i.e. native `memory`/`main` or any unmanaged catalog). `useQuery` treats those as always-ready.

### `useInvalidateQueries(pattern?)`

Invalidate all `useQuery` results, or only those whose SQL contains `pattern`.

## Server-side rendering (Node, RSC, edge)

Two pieces ship for running DuckDB on the server in addition to (or instead of) the browser WASM path:

- `@n6k.io/db/native/*` — thin wrapper around `@duckdb/node-api`. Opens a real DuckDB process, loads the n6k extension, runs ATTACH, returns the raw `DuckDBConnection`. Useful on its own for Node scripts and request handlers; does not depend on React.
- `@n6k.io/db/react` — `ServerDuckDBProvider` and `useSuspenseQuery` for rendering with data on the server.
- `@n6k.io/db/native-wasm-adapter/*` — `toWasmShape(conn)` translates a native `DuckDBConnection` into the same `{ schema, toArray() }` result shape the React hooks consume. Required when feeding a native conn into the providers/hooks.

`@duckdb/node-api` is an **optional** peer dependency; install it only if you import the `./native` subpath.

### `createNativeDuckDB(opts)` (from `@n6k.io/db/native/create-native-duckdb`)

```ts
import { createNativeDuckDB } from "@n6k.io/db/native/create-native-duckdb";

const { conn, dispose } = await createNativeDuckDB({
  path: "/path/to/file.duckdb",              // optional; in-memory if omitted
  n6kExtensionPath: "/abs/path/n6k_client.duckdb_extension", // optional; LOAD if set
  databases: { db: "n6k://api.example.com/p/123" },   // ATTACH each entry
});
try {
  // `conn` is the raw @duckdb/node-api DuckDBConnection.
  const reader = await conn.runAndReadAll("SELECT 1 AS x");
} finally {
  dispose();
}
```

`dispose()` is idempotent. The factory registers each undisposed conn in a `FinalizationRegistry` and warns in dev when one is GC'd without being disposed — wire the call into your request lifecycle (`try/finally`, framework `onResponse`, etc.).

### `ServerDuckDBProvider` (from `@n6k.io/db/react`)

Stateless context shim. Renders `<DuckDBContext.Provider>` with `status: "ready"`, the conn you pass, and `attached` seeded from the `databases` prop so dependency-tracking hooks (`useSuspenseQuery`, `useQuery`) see catalogs as ready. No `useEffect`, no client APIs — safe to use in RSC and `renderToString`/`renderToReadableStream`.

```tsx
import { renderToReadableStream } from "react-dom/server";
import { Suspense } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createNativeDuckDB } from "@n6k.io/db/native/create-native-duckdb";
import { toWasmShape } from "@n6k.io/db/native-wasm-adapter/adapter";
import {
  ServerDuckDBProvider,
  useSuspenseQuery,
} from "@n6k.io/db/react";

function UsersList() {
  const { rows } = useSuspenseQuery("SELECT id, name FROM db.main.users");
  return <ul>{rows.map((r) => <li key={String(r.id)}>{String(r.name)}</li>)}</ul>;
}

export async function handle(_req: Request) {
  const native = await createNativeDuckDB({
    n6kExtensionPath: process.env.N6K_EXTENSION_PATH,
    databases: { db: "n6k://api.example.com/p/123" },
  });
  try {
    const stream = await renderToReadableStream(
      <QueryClientProvider client={new QueryClient()}>
        <ServerDuckDBProvider conn={toWasmShape(native.conn)} databases={{ db: "n6k://api.example.com/p/123" }}>
          <Suspense fallback={<p>loading…</p>}>
            <UsersList />
          </Suspense>
        </ServerDuckDBProvider>
      </QueryClientProvider>,
    );
    await stream.allReady;
    return new Response(stream, { headers: { "content-type": "text/html" } });
  } finally {
    native.dispose();
  }
}
```

Props:

- `conn: ConnectionLike` — anything with `query(sql) → { schema, toArray() }`. Pass `toWasmShape(nativeConn)` for a `@duckdb/node-api` conn, or the wasm `AsyncDuckDBConnection` directly.
- `databases?: Record<string, ServerDatabaseSpec>` — same shape as `DuckDBProvider`'s `databases`. Seeds `desired` and `attached` in context so dependency-tracking hooks don't park in `awaitingAttach`. **Important**: this only updates context state; you still must have actually run the ATTACH against `conn` (the `createNativeDuckDB` factory does this for you via its own `databases` option).

`setDesired` / `removeDesired` are no-ops — dynamic attach during SSR has no re-render loop to settle on.

### `useSuspenseQuery(sql, options?)`

Same dependency-tracking, Decimal handling, and cache-key logic as `useQuery`, but delegates to tanstack's `useSuspenseQuery`. Throws the in-flight promise instead of returning `{ status: "pending" }`, so React's `<Suspense>` boundary waits for data before flushing HTML.

Returns `{ columns, rows, schema }` directly (no `status` / `error` — those propagate as thrown exceptions to your error boundary).

**When to use which hook:**

| Context | Hook |
| --- | --- |
| Client component that tolerates a loading state | `useQuery` |
| Component that must render with data during SSR (RSC, `renderToReadableStream`) | `useSuspenseQuery` + `<Suspense>` |

**Suspense has no `enabled` flag** — caller is responsible for:

1. The DuckDB conn being ready before the hook mounts. `ServerDuckDBProvider` makes this automatic; under the browser `DuckDBProvider`, gate the calling subtree on `status === "ready"`.
2. Every referenced catalog being attached before render. With `ServerDuckDBProvider`, declare it via the `databases` prop. A missing catalog throws a clear error (not a silent hang).

Plain `useQuery` under `ServerDuckDBProvider` returns `{ status: "loading" }` on first server render (no suspension); use `useSuspenseQuery` when you want data in the initial HTML.

## DuckDB in the parent, hooks in an iframe (`@n6k.io/db/react/iframe`)

Share one parent-window DuckDB-wasm instance with a child **iframe**, so the iframe's hooks (`useQuery`, `useExec`, `useView`, `useTableMeta`, …) work **without the iframe loading DuckDB at all**. The parent owns the single real engine (WASM, workers, the n6k extension, all sockets); the iframe is a thin reactive *view* — every query is a `postMessage` RPC to the parent that returns Arrow IPC bytes, rebuilt with `apache-arrow`'s `tableFromIPC`. Same supply-a-`conn` philosophy as `ServerDuckDBProvider`, but over a live channel.

The iframe bundle ships **only React + hooks + `apache-arrow`** — no `.wasm`, no worker, no n6k extension, no `SharedArrayBuffer`/COOP-COEP headers, no WebSocket. That guarantee is structural: import from the dedicated **`@n6k.io/db/react/iframe`** subpath (not `@n6k.io/db/react`, whose barrel re-exports `createDuckDB` and therefore duckdb-wasm).

```tsx
// PARENT — inside the normal <DuckDBProvider> (or any provider supplying a
// duckdb-wasm conn). Mount the host hook and point it at your iframe.
import { useRef } from "react";
import { DuckDBProvider } from "@n6k.io/db/react";
import { useDuckDBIframeHost } from "@n6k.io/db/react/iframe";

function Shell() {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  useDuckDBIframeHost(iframeRef, { allowedOrigins: [window.location.origin] });
  return <iframe ref={iframeRef} src="/widget" title="widget" />;
}

export function App() {
  return (
    <DuckDBProvider databases={{ db: "n6k://api.example.com/p/123" }}>
      <Shell />
    </DuckDBProvider>
  );
}
```

```tsx
// IFRAME app — no DuckDB engine here.
import { IFrameDuckDBProvider } from "@n6k.io/db/react/iframe";
import { useQuery } from "@n6k.io/db/react"; // hooks are wasm-free

function Widget() {
  // Pass explicit `tables` to skip the dependency-parse round-trip.
  const { rows } = useQuery("SELECT id, name FROM db.main.users", { tables: [{ catalog: "db", schema: "main", table: "users" }] });
  return <ul>{rows.map((r) => <li key={String(r.id)}>{String(r.name)}</li>)}</ul>;
}

export function Embedded() {
  return (
    <IFrameDuckDBProvider parentOrigin={window.location.origin}>
      <Widget />
    </IFrameDuckDBProvider>
  );
}
```

Handshake: the iframe posts `hello` (with a fresh per-mount `clientId`) to `parentOrigin`; the host validates the origin against `allowedOrigins` **and** that the message came from its own iframe element, then transfers a `MessageChannel` port. A new `clientId` (e.g. after an iframe reload) triggers a fresh port — `WindowProxy` identity is stable across reloads, so `clientId` is what distinguishes a reloaded frame from a duplicate hello. The host pushes the provider's attach state (`status`, `desired`, `attached`, …) so the iframe's `useQuery` catalog gate resolves instead of parking.

**Security — the server is the control level, not this bridge.** The iframe holds no token and no socket; it can only send SQL. It cannot exfiltrate a credential (n6k secrets are redacted in `duckdb_secrets()` and live in the parent's worker, unreachable from SQL) and cannot attach a private catalog (no credential to present). So there is no SQL-gating layer here. The only rule is a deployment one:

> The iframe can do exactly what the parent's already-attached catalogs allow — **including writes** (`INSERT`/`UPDATE`/`DELETE`) where the catalog's token grants them — and nothing more. Do not attach into an engine you share with an untrusted iframe anything you would not hand that iframe directly. (DuckDB `ATTACH` is global to the instance, so a shared engine cannot isolate the iframe from any attached catalog.)

Notes:

- `registerWebsocket` / `replaceWebsocket` **throw** in the iframe — sockets live with the parent's worker and can't cross frames.
- Each query without explicit `tables` costs one extra round-trip (`n6k_parse_sql_get_tables` dependency parsing). Pass `tables` to skip it.
- Same-origin parent/iframe is the supported configuration. Never use `"*"` as `parentOrigin` or in `allowedOrigins`.

## Debug logging

The driver is quiet by default. Info/debug logs are gated behind a runtime
flag; warnings and errors always print. All output is prefixed `[n6k]` on the
main thread and `[n6k:<name>-worker]` from workers (`duckdb-worker`,
`fetch-worker`, `ws-worker`).

Enable:

```js
localStorage.setItem("n6k:debug", "1"); // browser
globalThis.N6K_DEBUG = true; // any scope (incl. workers, SSR)
```

Disable: `localStorage.removeItem("n6k:debug")` and clear the global. Reload
to pick up the change in already-spawned workers.

## License

MIT
