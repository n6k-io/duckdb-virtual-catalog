# SQL API reference

Every function the two extensions register. All arguments are `VARCHAR` unless
noted. Scalar functions return `'ok'` except `bridge_register_source`, which
returns a token.

The prefix says which extension owns the name: `bridge_%` comes from
`virtual_catalog_bridge`, `provider_%` from `virtual_catalog_provider`. The two
share no SQL, so loading one gives you only its half.

## Bridge

The bridge is installed by
`ATTACH '' AS … (TYPE virtual_catalog_bridge, ID bridge_id, TOKEN token)`, after
the source has granted, and released by `DETACH`. Both options are required, and
neither may be a `?` parameter or a subquery: DuckDB folds ATTACH options to
constants at bind time.

Table names are `'schema.table'`, resolved in `source_catalog`. Each granted source
schema lands in the target schema of the same name.

| Function | Arguments |
|---|---|
| `bridge_register_source` | `bridge_id, source_catalog[, token]` → token. With two arguments the token is generated as `NONCE:UUID`; with three the source names it |
| `bridge_policy` | `bridge_id, 'schema.table', verb, policy_using[, policy_check]` — verb is `select\|insert\|update\|delete\|alter`; the same `(table, verb)` twice is an error. `policy_using` is mandatory (`'true'` = unrestricted) and says which rows the verb may reach. `policy_check` says what it may write, applies to `insert` and `update` only, and is a violation when it evaluates to NULL. Per verb: `update` defaults it to `policy_using` when omitted; `insert` must state it (`'true'` to opt out) because an insert has no USING to inherit; `select` and `delete` write no row and reject it. On `update` it is evaluated against the row as it will be *after* the update |
| `bridge_primary_key` | `bridge_id, 'schema.table', columns VARCHAR[]` — overrides discovery |
| `bridge_primary_key_query` | `bridge_id, 'schema.table', sql` — runs `sql` on the source; column 0 of every row is a key column, in key order |
| `bridge_primary_key_check` | `bridge_id, 'schema.table', sql` — runs `sql` on the source; zero rows accepts the declared key, any row rejects it |

## Providers

Requires `ATTACH ':memory:' AS … (TYPE virtual_catalog_provider)`. The named
UDFs must be registered on the same `DatabaseInstance` as the attached catalog.

| Function | Arguments |
|---|---|
| `provider_register` | `catalog, list_udf, schema_udf, scan_udf, insert_udf, update_udf, delete_udf, alter_udf` — empty string disables that verb; re-registration replaces |
| `provider_invalidate_tables` | `catalog` — bumps the version so table entries are re-listed |
| `provider_unregister` | `catalog` |

The same UDF set can ride on the ATTACH instead, as options `list`, `schema`,
`scan`, `insert`, `update`, `delete`, `alter`:

```sql
ATTACH '' AS app (TYPE virtual_catalog_provider, list p_list, schema p_schema, scan p_scan);
```

`list`, `schema` and `scan` go together — all three or none.

## Stream functions

| Function | Arguments |
|---|---|
| `provider_create_stream_function` | `catalog, schema, entry_name, open_udf, next_udf, close_udf` — re-registration replaces |
| `provider_drop_stream_function` | `catalog, schema, entry_name` |

The created entry is a table function taking arbitrary arguments (`ANY` varargs).
The three host UDFs it drives:

| UDF | Called as | Returns |
|---|---|---|
| `open_udf` | `open(handle, function_name, args_json, NULL::BLOB)` | Arrow IPC schema BLOB; NULL is an error |
| `next_udf` | `next(handle)` | Arrow IPC batch BLOB, or NULL at end of stream |
| `close_udf` | `close(handle)` | ignored |

`args_json` is the call's arguments as a JSON array; types with no JSON form
(dates, decimals, 128-bit ints) cross as strings. `handle` is a per-bind opaque
string.

## Introspection (table functions)

Both extensions register their own copy of this pair, under their own prefix:
`bridge_table_permissions` / `bridge_table_describe` and
`provider_table_permissions` / `provider_table_describe`. The signatures and
columns are identical; each recognises its own catalog type and classifies
anything else — including the *other* extension's catalogs, which live in a
separate binary — with the plain-DuckDB classifier.

`<prefix>_table_permissions(catalog, schema := …, "table" := …)` — named params
optional; `schema` defaults to `main`.

| Column | Type |
|---|---|
| `schema`, `name`, `kind` | VARCHAR |
| `primary_key`, `verbs` | VARCHAR[] |

`<prefix>_table_describe(catalog, schema := …, "table" := …)` — one row.
`"table"` is mandatory here; `schema` defaults to `main`.

| Column | Type |
|---|---|
| `columns` | STRUCT(name, type, nullable, default)[] |
| `primary_key`, `verbs` | VARCHAR[] |
| `enums` | MAP(VARCHAR, VARCHAR[]) |

`provider_stream_functions()` — no arguments.

| Column | Type |
|---|---|
| `catalog`, `schema`, `entry_name`, `function_name`, `open_udf`, `next_udf`, `close_udf` | VARCHAR |

`test/sql/bridge/virtual_catalog_bridge_api.test` and
`test/sql/provider/virtual_catalog_provider_api.test` pin these lists against
`duckdb_functions()`; adding or renaming a function fails there until this file
is updated. Each also asserts the *other* extension's prefix is absent, so a
name registered from the wrong side is caught immediately.

`provider_scan` and `bridge_seam` are internal: the former is the provider
table entry's own scan function and never reaches `duckdb_functions()`, the
latter is a table function the source runs and is not part of the user-facing
API.
