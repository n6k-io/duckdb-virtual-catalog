# virtual_catalog

A DuckDB extension that bridges two DuckDB `DatabaseInstance` objects in-process,
with per-table permissions. No network, no external server.

- Per-table permission model: `read` or `readwrite`
- Automatic PK discovery via `duckdb_constraints` or `information_schema`
- INSERT via DuckDB Appender, UPDATE/DELETE via prepared statements with rowid buffering
- Token-based setup handshake with nonce validation
- Projection and filter pushdown into the source connection
- Provider tables backed by host UDFs, and stream table functions over an Arrow IPC
  `open`/`next`/`close` triple

## Usage

```sql
LOAD virtual_catalog;

-- source connection: declare what the target may see
SELECT vcat_register_source('b1', 'memory', 'main',
                            MAP {'users': 'readwrite', 'logs': 'read'},
                            MAP {}::MAP(VARCHAR, VARCHAR));

-- target connection: consume the token, then attach
SELECT vcat_setup_bridge('b1', '<token>', 'app', 'main');
ATTACH ':memory:' AS app (TYPE virtual_catalog);

SELECT * FROM app.main.users;
```

Introspection:

```sql
SELECT * FROM vcat_table_permissions('app');
SELECT * FROM vcat_stream_functions();
```

See [docs/virtual-catalog.md](docs/virtual-catalog.md) for the full SQL API.

## Building

Prerequisites: CMake and a C++11 compiler. Submodules must be checked out:

```sh
git submodule update --init --recursive
make release
```

Output:

```
build/release/extension/virtual_catalog/virtual_catalog.duckdb_extension
```

## Testing

```sh
make test
```

Runs the sqllogictests under `test/sql/`.

## Formatting

```sh
make format-fix    # clang-format (make format-check to verify)
make tidy-check    # clang-tidy
```

Both need `black` and the clang tools on PATH.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for system design, data flow, and key file map.
