# virtual_catalog_provider

In-process DuckDB storage extension: a catalog whose tables are answered by
scalar UDFs the host process registered, rather than read from a file. The host
declares a shape once and serves rows as Arrow IPC; DuckDB sees ordinary table
entries.

No network, no external server. Pure SQL API, usable from any DuckDB binding
that can register a UDF.

Its sibling, [`virtual_catalog_bridge`](virtual-catalog-bridge.md), mirrors
tables from another `DatabaseInstance` instead. The two are separate extensions
with separate catalog types and no shared SQL; either can be loaded without the
other, and a single attached catalog is one or the other, never both.

## Setup

```sql
ATTACH '' AS app (TYPE virtual_catalog_provider,
                  list p_list, schema p_schema, scan p_scan,
                  insert p_insert, update p_update, delete p_delete, alter p_alter);
```

`list`, `schema` and `scan` are required together; the write verbs are optional.
An `ATTACH` naming none of them leaves the catalog provider-less until
`provider_register` fills it in:

```sql
ATTACH ':memory:' AS app (TYPE virtual_catalog_provider);

SELECT provider_register(
  'app',
  'p_list', 'p_schema', 'p_scan',
  'p_insert', 'p_update', 'p_delete', 'p_alter');
-- → 'ok'
```

The last seven arguments name UDFs already registered on the **same**
`DatabaseInstance`. The extension opens its own internal `Connection` against
that instance to call them, which is why host UDFs and the attached catalog
have to live on one connection's database rather than two.

**An empty string disables that verb.** A provider with `''` for `insert_udf`
is read-only, and the refusal is a `PermissionException` at bind time rather
than a failure once rows are already moving.

Re-registering the same catalog replaces the UDF set — that is the supported way
for a host to swap its implementation. (The bridge registry refuses a duplicate
id instead, because there a collision means two callers wanted the same name.)

Registration order is free: ATTACH, then any mix of `provider_register`,
`CREATE TABLE`, `CREATE SCHEMA`.

## Teardown

```sql
SELECT provider_unregister('app');
-- → 'ok'
```

It errors when no provider is registered for that catalog — including on a
second call. The catalog is detached from the provider before the registry entry
is dropped, so plan compilation racing the call stops routing there rather than
reaching a half-freed provider. `DETACH` does the same on its way out.

## The UDF contract

| UDF | Called as | Returns |
|---|---|---|
| `list_udf` | `list()` | `'\|'`-separated `schema.table` names |
| `schema_udf` | `schema(table_name)` | one Arrow IPC **schema** message |
| `scan_udf` | `scan(table_name, columns VARCHAR[], filters_json)` | Arrow IPC stream of the rows |
| `insert_udf` | `insert(table_name, arrow_ipc)` | affected row count |
| `update_udf` | `update(table_name, arrow_ipc, changed_columns)` | affected row count |
| `delete_udf` | `delete(table_name, arrow_ipc)` | affected row count |
| `alter_udf` | `alter(table_name, kind, details_json)` | ignored |

One provider serves the whole catalog. `list_udf` qualifies every name as
`schema.table`, an unqualified name is an error, and `table_name` in every other
UDF is that same qualified name. Schemas the list names are materialised on
demand, so a provider reaches a schema no SQL ever created.

Arrow IPC is the wire format because the payload crosses a SQL boundary as a
`BLOB`. Arguments are **bound, never spliced**: a table name or filter literal
containing a quote is a value, not SQL text. `make check-no-sql` enforces that
for the whole source tree.

### Declaring a shape

`schema_udf` returns exactly what `pa.Schema.serialize()` produces. Column names
and DuckDB types both come out of it, so the provider declares its shape once.

Primary keys ride in the schema's metadata under `vcat.primary_keys` (the legacy
`n6k.primary_keys` key is still accepted). `UPDATE` and `DELETE` require one —
they address rows by key, and a provider that declares none is refused at bind
time with "has no primary key declared".

### Projection and filter pushdown

`scan_udf` receives the projected column list and a JSON rendering of the
`WHERE` clause. Supported: `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `is_null`,
`is_not_null`, and `AND` conjunctions of those.

**`OR` is refused, not silently flattened.** The flat pyarrow tuple shape cannot
express a disjunction, and a filter that cannot be rendered exactly makes the
scan throw rather than fall back to filtering locally — the provider must never
be handed a predicate narrower than the one the user wrote.

Literals that cross as text carry a rebuild tag, so the callee does not have to
re-fetch the schema on every filtered scan to learn that `'2026-05-01'` was a
`DATE`.

A `COUNT(*)` projects nothing. Arrow can carry a batch with no columns, but a
pyarrow `Table` built from an empty array list reports zero rows however many
matched, so one column is requested and discarded; only the cardinality is read.

### What the scan returns

Columns come back as *projection first, key columns trailing*. A key that is
also projected is requested twice on purpose — the row-id machinery reads the
trailing block positionally.

The result is checked against what `schema_udf` declared. A short result or a
column returned as the wrong Arrow type is an error naming the table and column,
not a silent misread: reading a `utf8` buffer where `INTEGER` was declared would
otherwise return whatever the offsets happened to be.

## Writing

`INSERT`, `UPDATE` and `DELETE` are routed through a phantom
`ProviderTableCatalog` to the matching UDF, with rows encoded as Arrow IPC.

`UPDATE` and `DELETE` are keyed: the scan appends each row's primary-key values
to a shared buffer and emits the buffer index in the `rowid` slot, and the write
resolves those indices back to key values at finalize.

Every keyed write is checked as it runs — if the provider reports more affected
rows than the statement sent key values, the declared key is not unique and the
write is refused. The message never names the offending value, since a permitted
write is already an existence oracle over hidden rows.

**Not supported:** the `RETURNING` clause on INSERT/UPDATE/DELETE, and
`CREATE TABLE AS` into a provider schema.

## ALTER and invalidation

`ALTER TABLE` reaches `alter_udf` for `ADD COLUMN`, `DROP COLUMN` and
`RENAME COLUMN`; anything else is refused. A provider registered with an empty
`alter_udf` refuses all of them. The altered table's cached entry is evicted
afterwards, but *retired rather than freed* — a concurrent scan may still hold a
raw pointer to it.

`DROP TABLE` is refused outright: the entry stands for something in a backing
store the extension does not own. Modify the store, then:

```sql
SELECT provider_invalidate_tables('app');
-- → 'ok'
```

That bumps a version counter the schema wrapper compares against on next access,
so the table list and every cached entry are re-fetched. Nothing else exposes
the counter.

## Stream functions

Table functions, rather than tables, backed by an Arrow IPC `open`/`next`/`close`
triple:

```sql
SELECT provider_create_stream_function('app', 'main', 'events',
                                       'ev_open', 'ev_next', 'ev_close');
SELECT * FROM app.main.events('since', 42);
```

The created entry takes arbitrary arguments (`ANY` varargs), delivered to
`open_udf` as a JSON array. Types with no JSON form — dates, decimals, 128-bit
ints — cross as strings. `handle` is a per-bind opaque string, so two concurrent
calls to one function do not share state.

`next_udf` returns a batch, or NULL at end of stream. A batch narrower than the
schema `open_udf` declared is refused.

`provider_drop_stream_function` removes one; `provider_stream_functions()` lists
what is registered. Re-creating an existing name replaces it.

## Introspecting a provider

`provider_table_permissions` reports, per table/view in an attached catalog,
which statements it accepts. It works on a `virtual_catalog_provider` catalog
and on a plain DuckDB catalog. Signature and columns:
[`sql-api.md`](sql-api.md).

`kind` is one of `native_table` · `native_view` · `provider`. Capability by
entry kind:

| `kind` | `verbs` | `primary_key` |
|--------|---------|---------------|
| `native_table` | all five | from table constraints |
| `native_view` | `select` | empty |
| `provider` | `select`, plus one per implemented UDF | from `vcat.primary_keys` Arrow metadata (legacy `n6k.primary_keys` still accepted) |

A `virtual_catalog_bridge` catalog is a different extension's binary, so this
function does not recognise it and falls back to the plain-catalog classifier.
Use `bridge_table_permissions` for those; it is the same function compiled into
the bridge.

`verbs` is the single source of truth for capability. There is deliberately no
coarse `writeable`/`editable` pair alongside it. A `READ_ONLY` attach narrows
`verbs` to `select` whatever the entry's own type says, because the engine will
refuse the write regardless and the client should not learn that only on
failure.

It is a metadata lookup: it reads the provider's registered UDF names, not the
UDFs themselves, so listing permissions never calls into the host. Filling a
table's `primary_key` does call `schema_udf`, since that is where keys are
declared.

`kind` is an open string set: an extension layered on top of this one may report
values not listed above for catalog types it owns.

The optional `schema` / `"table"` named parameters are case-insensitive filters.

### Single-table descriptor: `provider_table_describe`

Returns one fully-typed row for a single table. It reuses the same per-kind
capability collectors as `provider_table_permissions` for the verbs and primary
key, and reads columns / enum domains from the catalog entry — so a client gets
everything it needs to render a table in one call instead of stitching together
`DESCRIBE`, `duckdb_constraints()`, and per-column `enum_range()`.

## Unified catalog (`TYPE virtual_catalog_provider`)

Native DuckDB tables and provider tables coexist freely in the same schema:

```sql
ATTACH ':memory:' AS workspace (TYPE virtual_catalog_provider);
```

`workspace` behaves exactly like a native in-memory DuckDB catalog — full native
storage and ACID transactions via DuckDB's own `DuckTransactionManager`. On top
of that, the same schema can host provider entries. Operations route by origin:

| Op on... | Native entry | Provider entry |
|---|---|---|
| SELECT / scan | native | `scan_udf` |
| INSERT / UPDATE / DELETE | native | routed via phantom `ProviderTableCatalog` → provider |
| ALTER TABLE | native | `alter_udf` (ADD/DROP/RENAME COLUMN) |
| DROP TABLE | native | rejected — modify the backing store, then `provider_invalidate_tables` |
| CREATE TABLE / CREATE VIEW | native — forwarded to wrapped DuckSchemaEntry | n/a |

A name collision resolves native-first: the wrapped schema is consulted before
the provider's table list, so a `CREATE TABLE` shadows a provider table of the
same name rather than colliding with it.

Bridge entries cannot live here — they need `TYPE virtual_catalog_bridge`, a
separate catalog from a separate extension.

## See also

- [`sql-api.md`](sql-api.md) — every registered function, its arguments and columns.
- [`virtual-catalog-bridge.md`](virtual-catalog-bridge.md) — the sibling extension.
- [`../ARCHITECTURE.md`](../ARCHITECTURE.md) — catalog architecture, write path,
  registries, and thread safety.
