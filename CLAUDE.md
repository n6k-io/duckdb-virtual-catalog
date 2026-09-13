# Build & Test

The USER runs builds — never run them yourself. Pick the right command for what you changed:
- **`make release`** builds the **native** extension only (`build/release/`). Use for native
  (`BACKEND=native`) changes.
- **`make wasm`** (alias `make wasm-all`) builds the **coi/wasm_threads** extension AND stages it
  into `packages/npm/wasm/` — the bundle `BACKEND=browser-threads` actually loads. `make release`
  does NOT rebuild or stage the wasm bundle, so **any C++ change under a wasm/`WITH_WASM_THREADS`
  path requires the user to run `make wasm`**, not `make release`.
- After a wasm build, confirm the staged bundle is fresh before trusting a browser-threads run,
  e.g. grep a change-unique string: `strings packages/npm/wasm/*/wasm_threads/n6k.duckdb_extension.wasm | grep -F "<your new literal>"`.

## Shell rules
- Always run `cd` in its own Bash command, never chained with `&&`.
- Bash cwd persists between calls — track where you are before running the next command.

## Formatting & Tidy (C++ only)
Check `which python3` — if it's not `.venv/bin/python3`, activate the
venv in the same command:
```
source .venv/bin/activate && make format-fix
source .venv/bin/activate && make tidy-check
```
Bash state doesn't persist between tool calls, so re-activate on each
new invocation.
Runs `clang-format` and `clang-tidy` against `src/ test/`. Does NOT
touch Python code.

## Python lint / type-check
`black` and `flake8` run from either the repo root or `packages/python/`.
The root config skips submodules, vendored `node_modules`, and `scratch/`;
the package config has its own strict settings.

`mypy` must run from `packages/python/` — mypy resolves imports relative
to CWD, and running it with a directory arg from root doesn't discover the
package's files the same way.
```
uv run black .
uv run flake8 .
cd packages/python && uv run mypy . --strict
```

## Tests (use uv, no sandbox)
```
uv run pytest packages/python           # colocated unit tests under src/n6k_protocol/**/__tests__/ and src/n6k_server/**/__tests__/
uv run pytest integration_tests/        # requires the test server running on :8099
uv run pytest packages/python --external "postgres://sean@localhost/n6k_test"
```

## Bun tests (packages/npm) — NO SANDBOX
DuckDB-wasm writes extensions under `<HOME>/.duckdb/extensions/...`, which the
sandbox blocks (`EPERM mkdir`). Always run bun with sandbox disabled.

Use `bun run test`, NOT a bare `bun test`: the `test` script sets
`HOME=$(mktemp -d)` so duckdb-wasm's on-disk extension cache lands in a fresh
throwaway dir. duckdb-wasm keys that cache by repo `host:port` and trusts a hit
without revalidating, and the harness serves the extension on a random port —
so a shared `~/.duckdb` accumulates per-port copies and a port collision can
load a STALE extension (the `n6k_parse_sql_get_tables` "privilege_type not
found" flake). An isolated empty HOME forces a fresh fetch every run.
`SET extension_directory`/`home_directory` and `FORCE INSTALL` do NOT relocate
this cache in the node runtime — only the OS `HOME` does. The wasm harness hard
-errors (`N6K_ISO_HOME` unset) if you bypass this.
```
cd packages/npm
# TEST_SERVER is required: a base URL to run server tests against, or `skip`.
TEST_SERVER=http://localhost:8099 BACKEND=native bun run test          # real run (native ext)
TEST_SERVER=http://localhost:8099 BACKEND=browser-threads bun run test # real run (coi wasm, Playwright)
TEST_SERVER=skip BACKEND=skip bun run test                             # no server (CI)
```
`TEST_SERVER` (in `src/__tests__/_server-gate.ts`) is the single source for
both the server URL and the skip gate — every server-requiring test derives its
http/ws/n6k URLs from it. `BACKEND` (native|browser-threads|skip) selects the
engine for the unified backend tests (wasm is coi-only, so the eh/node
`wasm-node`/`browser` backends were retired). Either env set to `skip` skips the
server-requiring tests; both must be set (unset is a hard error).

## Protocol constants
Single source of truth: `packages/python/src/n6k_protocol/protocol.py`. After
editing, run `make protocol-gen` and commit the regenerated TS + C++ mirrors.
`make protocol-check` verifies the committed outputs match (run it by hand).

## Python duckdb version must match extension build (currently v1.5.4)

## Server framework

The n6k protocol server framework lives in two packages under
`packages/python/src/`:

Python does **not** implement the protocol — `src/n6k_server` (C++) does. Python
accepts the WebSocket, ATTACHes the catalogs, hands the socket to
`CALL n6k_serve_fd(<fd>, ...)`, and pumps bytes.

- `n6k_protocol/` — the wire format and its codec, no FastAPI/Starlette/duckdb
  dependency: constants + the codegen SSOT (`protocol.py`), and
  `pack_frame`/`unpack_frame`/`validate_header` plus the handshake helpers
  (`engine.py`). Used by clients and wire tests, not by the server.
- `n6k_server/` — adapters and consumers: `pump.py` (the byte pump),
  `server_fastapi/` (the FastAPI adapter, `register(app, prefix, *, connect=...)`),
  `provider.py`, and the test server (`test_server/`).

Consumers bring their own FastAPI app and call `register()` with a `connect`
factory returning a DuckDB connection; every catalog it ATTACHed is served.
Defining `n6k_authorize(token, catalog) -> BOOLEAN` on that connection turns on
handshake auth. Sibling production servers (`data-service`, `server`) are the
intended future consumers.
