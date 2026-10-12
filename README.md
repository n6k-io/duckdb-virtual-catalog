# virtual_catalog

Two DuckDB extensions that serve a catalog from somewhere other than a file. No network, no
external server.

- `virtual_catalog_bridge` bridges two DuckDB `DatabaseInstance` objects in-process, with
  per-table permissions.
- `virtual_catalog_provider` backs tables with host-registered UDFs.

- Per-table, per-verb grants (`select`/`insert`/`update`/`delete`/`alter`) with optional row policies
- Automatic PK discovery off the source table's `PRIMARY KEY` constraint
- INSERT via DuckDB Appender, UPDATE/DELETE via prepared statements over a shared PK buffer
- Token-based setup handshake with nonce validation
- Projection and filter pushdown into the source connection
- Provider tables backed by host UDFs, and stream table functions over an Arrow IPC
  `open`/`next`/`close` triple

## Usage

```sql
LOAD virtual_catalog_bridge;

-- source connection: mint a token, then grant one verb at a time
SELECT bridge_register_source('b1', 'memory');
SELECT bridge_set_context('b1', 'tenant', 42);
SELECT bridge_policy('b1', 'main.users',   'select', 'true');
SELECT bridge_policy('b1', 'main.users',   'update', 'tenant_id = $tenant');   -- reaches, and writes, the tenant
SELECT bridge_policy('b1', 'main.users',   'insert', 'true', 'tenant_id = $tenant');
SELECT bridge_policy('b1', 'sales.orders', 'select', 'true');

-- target connection: the ATTACH redeems the token; app.main and app.sales are created as needed
ATTACH '' AS app (TYPE virtual_catalog_bridge, ID b1, TOKEN '<token>');

SELECT * FROM app.main.users;
SELECT * FROM app.sales.orders;

DETACH app;
```

Introspection:

```sql
SELECT * FROM bridge_table_permissions('app');
SELECT * FROM provider_stream_functions();
```

Every function is listed in [docs/sql-api.md](docs/sql-api.md);
[docs/virtual-catalog-bridge.md](docs/virtual-catalog-bridge.md) and
[docs/virtual-catalog-provider.md](docs/virtual-catalog-provider.md) explain the model behind them.

## Building

Prerequisites: CMake and a C++14 compiler. Submodules must be checked out:

```sh
git submodule update --init --recursive
make release
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
