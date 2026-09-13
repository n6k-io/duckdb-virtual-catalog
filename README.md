# virtual_catalog

A DuckDB extension that bridges two DuckDB `DatabaseInstance` objects in-process,
with per-table permissions. No network, no external server.

- Per-table, per-verb grants (`select`/`insert`/`update`/`delete`/`alter`) with optional row policies
- Automatic PK discovery off the source table's `PRIMARY KEY` constraint
- INSERT via DuckDB Appender, UPDATE/DELETE via prepared statements over a shared PK buffer
- Token-based setup handshake with nonce validation
- Projection and filter pushdown into the source connection
- Provider tables backed by host UDFs, and stream table functions over an Arrow IPC
  `open`/`next`/`close` triple

## Usage

```sql
LOAD virtual_catalog;

-- target connection: attach first, bridges are added to an existing catalog
ATTACH ':memory:' AS app (TYPE virtual_catalog);

-- source connection: mint a token, then grant one verb at a time
SELECT vcat_register_source('b1', 'memory');
SELECT vcat_policy('b1', 'main.users',   'select', 'true');
SELECT vcat_policy('b1', 'main.users',   'update', 'tenant_id = 42');   -- reaches, and writes, tenant 42
SELECT vcat_policy('b1', 'main.users',   'insert', 'true', 'tenant_id = 42');
SELECT vcat_policy('b1', 'sales.orders', 'select', 'true');

-- target connection: consume the token; app.main and app.sales are created as needed
SELECT vcat_setup_bridge('b1', '<token>', 'app');

SELECT * FROM app.main.users;
SELECT * FROM app.sales.orders;
```

Introspection:

```sql
SELECT * FROM vcat_table_permissions('app');
SELECT * FROM vcat_stream_functions();
```

Every function is listed in [docs/sql-api.md](docs/sql-api.md);
[docs/virtual-catalog.md](docs/virtual-catalog.md) explains the model behind them.

## Building

Prerequisites: CMake and a C++14 compiler. Submodules must be checked out:

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
