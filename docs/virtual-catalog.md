# virtual_catalog

In-process DuckDB storage extension: a catalog whose contents come from callbacks
or from another `DatabaseInstance` in the same process, rather than from a file.
Two features share the binary:

- **the bridge** — a **source** `DatabaseInstance` exposes selected tables to a
  **target** `DatabaseInstance` with per-table permissions;
- **providers and stream functions** — tables and table functions backed by
  scalar UDFs the host process registered.

No network, no external server. Pure SQL API, usable from any DuckDB binding
(C++, Python, Go, Node, etc.).

## Setup flow

Token-based handshake, all plain SQL statements; the target uses a different
connection than the source.

`vcat_setup_bridge` resolves the target catalog and schema, so **both must
already exist when it runs** — the ATTACH and the CREATE SCHEMA come first, not
after.

```sql
-- 1. On the TARGET: attach the catalog the bridge will be injected into.
ATTACH ':memory:' AS my_bridge (TYPE virtual_catalog);
CREATE SCHEMA IF NOT EXISTS my_bridge.main;

-- 2. On the SOURCE: register with permissions, receive a one-time token.
SELECT vcat_register_source(
    'my_bridge',                                    -- bridge_id
    'source_catalog',                               -- source catalog name
    'main',                                         -- source schema name
    MAP {'users': 'readwrite', 'logs': 'read'},     -- permissions
    MAP {'users': 'id', 'logs': 'id,timestamp'}     -- pk_overrides (optional, use MAP() for none)
);
-- → 'NONCE:UUID'

-- 3. On the TARGET: redeem the token and inject the bridge schema.
SELECT vcat_setup_bridge(
    'my_bridge',    -- bridge_id, matching step 2
    '<token>',      -- the token step 2 returned
    'my_bridge',    -- target catalog name (the ATTACH alias)
    'main'          -- target schema
);
-- → 'ok'
```

The token contains a nonce unique to the loaded extension binary — mismatched
extension versions are rejected. Tokens expire after **30 seconds** if not
consumed. Permissions are declared by the source (trusted side) and cannot be
modified by the target.

## Teardown

```sql
-- On the TARGET:
SELECT vcat_unregister_bridge('my_bridge', 'main');   -- (catalog, schema)
-- → 'ok'
```

Not optional in a long-lived process. The bridge registry holds a reference to
the **source** `DatabaseInstance`, so until this runs, `DETACH` on either side
frees nothing and the `bridge_id` stays claimed. Unregistering releases both,
making the id reusable.

It errors if no bridge is bound to that `(catalog, schema)` — including on a
second call, which is how it reports that there was nothing to release.

A refused `vcat_setup_bridge` (an id still in use, say) leaves the pending
source from step 2 intact, so the same token still works once the id is freed;
you do not have to go back to the source connection to re-register.

## Permission model

Per-table, specified as the fourth argument (`MAP(VARCHAR, VARCHAR)`) to
`vcat_register_source`:

| Permission | Example | Behavior |
|------------|---------|----------|
| `'read'` | `MAP {'t1': 'read'}` | Read-only access |
| `'readwrite'` | `MAP {'t2': 'readwrite'}` | Read, INSERT, UPDATE, and DELETE access |

Tables not listed in permissions are inaccessible (`CatalogException`).

## Primary keys

Writable (`readwrite`) tables require a primary key. The bridge discovers PKs
automatically via `duckdb_constraints()` or `information_schema.key_column_usage`
(MySQL). If auto-discovery fails, provide explicit overrides in the
`pk_overrides` parameter — a `MAP(VARCHAR, VARCHAR)` mapping table names to
comma-separated PK column names:

```sql
MAP {'users': 'id', 'events': 'tenant_id,event_id'}
```

Tables marked `readwrite` without a discoverable or overridden PK will raise
an error at setup time.

## Which source schema a bridge exposes

Permission keys are **bare table names** — a key containing a dot is rejected:

```
virtual_catalog: permission key 'shop.main.orders' must be a bare table name (no dots)
```

Every key resolves inside the catalog and schema named in arguments 2 and 3 of
`vcat_register_source`, so that pair is what picks the source schema:

```sql
-- exposes shop.main.orders, not memory.main.orders
SELECT vcat_register_source(
    'b1', 'shop', 'main',
    MAP {'orders': 'read'},
    MAP {}::MAP(VARCHAR, VARCHAR)
);
```

One bridge therefore exposes exactly one source schema. To expose tables from a
second source catalog or schema, register a second bridge — its own `bridge_id`
and its own target schema, since a bridge binds to one `(target catalog, schema)`
pair.

## Reading

Tables with READ (or READWRITE) permission are exposed as views wrapping the
`vcat_scan` table function:

```sql
SELECT * FROM vcat_scan('my_bridge', 'users');
```

Supports the same pushdown as the network protocol:

- **Projection** — only requested columns are fetched from source
- **Filters** — `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `is_null`, `is_not_null`

Internally builds a SQL query on the source with pushed-down columns and WHERE
clause.

## Writing

Tables with READWRITE permission support `INSERT INTO`, `UPDATE`, and `DELETE`.

**INSERT** uses DuckDB's `Appender` API on the source:

1. Incoming data chunks are accumulated during the sink phase
2. On finalize, an `Appender` writes all chunks to the source table
3. The appender commits on the source immediately

**UPDATE** and **DELETE** use rowid-based operations:

1. The scan includes `rowid` as an extra column
2. Affected rows are collected during the sink phase
3. On finalize, UPDATE/DELETE SQL is executed on the source using the collected rowids

Write permission is validated at plan time and re-checked at finalize
(defense-in-depth).

**Not yet supported:** `CREATE TABLE`, `RETURNING` clause on INSERT/UPDATE/DELETE.

## Introspecting permissions

`vcat_table_permissions` reports, per table/view in an attached catalog, whether
its **data** is writeable (INSERT/UPDATE/DELETE) and whether its **columns** are
editable (ALTER TABLE ADD/DROP/RENAME COLUMN). It works on a `virtual_catalog`
catalog and on a plain DuckDB catalog:

```sql
SELECT * FROM vcat_table_permissions('app');                      -- all schemas
SELECT * FROM vcat_table_permissions('app', schema := 'main');    -- one schema
SELECT * FROM vcat_table_permissions('app', schema := 'main', "table" := 'users');
```

Output columns:

| Column | Type | Meaning |
|--------|------|---------|
| `schema` | VARCHAR | Schema name |
| `name` | VARCHAR | Table / view name |
| `kind` | VARCHAR | `native_table` · `native_view` · `bridge_read` · `bridge_readwrite` · `provider` |
| `writeable` | BOOLEAN | Data can be added / modified / removed |
| `editable` | BOOLEAN | Columns can be added / removed / changed |
| `primary_key` | VARCHAR[] | Primary-key column names in key order; empty if none |

Capability by entry kind:

| `kind` | `writeable` | `editable` | `primary_key` |
|--------|-------------|------------|---------------|
| `native_table` | true | true | from table constraints |
| `native_view` | false | false | empty |
| `bridge_read` | false | false | empty (READ tables skip PK discovery) |
| `bridge_readwrite` | true | false (bridge ALTER is unsupported) | discovered from source |
| `provider` | provider implements `insert` | provider implements `alter` | from `vcat.primary_keys` Arrow metadata |

For an `virtual_catalog` catalog it is a fast, in-memory metadata lookup: it reads
the bridge permission map, the provider's registered UDFs, and the native
catalog type directly — no source-DB query, and no provider UDF call to
enumerate columns. On a plain DuckDB catalog it classifies native tables/views
from the catalog directly.

`kind` is an open string set: an extension layered on top of this one may report
values not listed above for catalog types it owns.

The optional `schema` / `"table"` named parameters are case-insensitive filters.

### Single-table descriptor: `vcat_table_describe`

`vcat_table_describe('catalog', schema := 'main', "table" := 'users')` returns one
fully-typed row for a single table — `columns` (a `STRUCT(name, type, nullable,
"default")[]`), `primary_key`, `writeable`, `editable`, and `enums`
(`MAP(VARCHAR, VARCHAR[])`). It reuses the same per-kind permission collectors as
`vcat_table_permissions` for the capability bits and primary key, and reads columns
/ enum domains from the catalog entry — so a client gets everything it needs to
render a table in one call instead of stitching together `DESCRIBE`,
`duckdb_constraints()`, and per-column `enum_range()`.

## Transaction semantics

- Stateless — commit and rollback on the target are no-ops
- No snapshot isolation — target always sees the source's latest committed data
- Inserts commit on the source independently of the target transaction

## Unified catalog (`TYPE virtual_catalog`)

The `virtual_catalog` storage extension provides a unified catalog type where
native DuckDB tables, bridge-mirrored tables, and Python-provider tables
coexist freely in the same schema:

```sql
ATTACH ':memory:' AS workspace (TYPE virtual_catalog);
```

`workspace` behaves exactly like a native in-memory DuckDB catalog — full
native storage and ACID transactions via DuckDB's own `DuckTransactionManager`.
On top of that, the same schema can host bridge and provider entries.
Operations route by origin:

| Op on... | Native entry | Provider entry | Bridge entry |
|---|---|---|---|
| SELECT / scan | native | `provider.scan` | bridge source query |
| INSERT / UPDATE / DELETE | native | routed via phantom `ProviderTableCatalog` → provider | routed via phantom `BridgeCatalog` → bridge |
| ALTER TABLE | native | `provider.alter` (ADD/DROP/RENAME COLUMN) | rejected |
| DROP TABLE | native | rejected: "modify backing store + invalidate" | rejected |
| CREATE TABLE / CREATE VIEW | native — forwarded to wrapped DuckSchemaEntry | n/a | n/a |

Registration order is free: ATTACH, then any mix of `register_provider(...)`,
bridge handshake, `CREATE TABLE`, `CREATE SCHEMA`.

Provider entries are a Python-specific construct — see the Python API docs.

## Using from other languages

Any DuckDB binding that can execute SQL can drive the handshake directly. The
statements in [Setup flow](#setup-flow) and [Teardown](#teardown) are the
complete API surface; there are no out-of-band calls.

Rough sketch in Go (pseudocode):

```go
// target connection: the catalog and schema must exist before the bridge is set up
targetDB.Exec(`ATTACH ':memory:' AS my_bridge (TYPE virtual_catalog)`)
targetDB.Exec(`CREATE SCHEMA IF NOT EXISTS "my_bridge"."main"`)

// source connection
var token string
sourceDB.QueryRow(`
    SELECT vcat_register_source(?, ?, ?, ?::MAP(VARCHAR, VARCHAR), ?::MAP(VARCHAR, VARCHAR))
`, bridgeID, sourceCatalog, sourceSchema, permissions, pkOverrides).Scan(&token)

// target connection
targetDB.Exec(`SELECT vcat_setup_bridge(?, ?, ?, ?)`, bridgeID, token, "my_bridge", "main")

// ... and on the way out, so the source DatabaseInstance is released
targetDB.Exec(`SELECT vcat_unregister_bridge(?, ?)`, "my_bridge", "main")
```

Identical code works from Node (duckdb-node), Rust (duckdb-rs), C++, Java, etc.

## See also

- [`../ARCHITECTURE.md`](../ARCHITECTURE.md) — catalog architecture, write path,
  registries, and thread safety.
