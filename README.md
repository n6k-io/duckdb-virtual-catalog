# virtual_catalog

Two DuckDB extensions that serve a catalog from somewhere other than a file. No network, no
external server.

- `virtual_catalog_bridge` bridges two DuckDB `DatabaseInstance` objects in-process, with
  per-table permissions.
- `virtual_catalog_provider` backs tables with host-registered UDFs.

- Per-table, per-verb grants with optional row policies, set as one JSON document or a setting at a time
- Automatic PK discovery off the source table's `PRIMARY KEY` constraint
- INSERT via DuckDB Appender, UPDATE/DELETE via prepared statements over a shared PK buffer
- Token-based setup handshake with nonce validation
- Projection and filter pushdown into the source connection
- Provider tables backed by host UDFs, and stream table functions over an Arrow IPC
  `open`/`next`/`close` triple

## Usage

```sql
LOAD virtual_catalog_bridge;

-- source connection: mint a token, then set the policy
SELECT bridge_register_source('b1', 'memory');
SELECT bridge_set_context('b1', 'tenant', 42);
SELECT bridge_configure_policy('b1', '{
  "policy_version": 1,
  "tables": {
    "main.users":   {"select": true, "update": "tenant_id = $tenant", "insert": "tenant_id = $tenant"},
    "sales.orders": {"select": true}
  }
}');
-- or one setting at a time
SELECT bridge_set_policy('b1', 'main.users', 'delete', 'tenant_id = $tenant');

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
