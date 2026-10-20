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

The policy is set with two functions, before or after `ATTACH`, until
`bridge_finalize_policy`; see [policy](virtual-catalog-bridge.md#policy).

| Function | Arguments |
|---|---|
| `bridge_register_source` | `bridge_id, source_catalog[, token]` → token. With two arguments the token is generated as `NONCE:UUID`; with three the source names it |
| `bridge_set_context` | `bridge_id, name, value ANY` — a value policies reference as `$name`. Must precede the policy that uses it. Works before and after `ATTACH`; overwrites, and the target's next statement uses the new value. Refused when the value breaks a policy that uses it, when NULL, or after `bridge_finalize_context` |
| `bridge_configure_policy` | `bridge_id, policy JSON` — replaces the whole policy with the document. Checked in full before anything changes. Schema: [`policy-v1.schema.json`](policy-v1.schema.json) |
| `bridge_validate_policy_config` | `source_catalog, policy JSON` → table. Checks a document against the catalog without a bridge; see [below](#policy-validation) |
| `bridge_set_policy` | `bridge_id, target, setting, value ANY` — assigns one setting. `NULL` removes it; for a verb, so does `false` |
| `bridge_set_policy` | `bridge_id, target, setting, op, value ANY` — changes a list setting. `op` is `add`, `remove` or `set`; `value` is a name or a list of names (`set` takes a list) |
| `bridge_finalize_context` | `bridge_id` — optional; freezes the context, before or after `ATTACH` |
| `bridge_finalize_policy` | `bridge_id` — optional; freezes the policy, before or after `ATTACH`. Every later `bridge_configure_policy` and `bridge_set_policy` is refused |
| `virtual_catalog_lockdown` | `memory_limit, threads BIGINT[, drop_functions VARCHAR[]]` — run on the target; locks its whole `DatabaseInstance`. First drops each named function from the system catalog (every scalar, aggregate, macro, table and pragma entry of that name, e.g. `['quack_serve', 'quack_stop']`). Sets `memory_limit` and `threads`, disables spilling (`temp_directory = ''`), extension autoload/autoinstall, community extensions, persistent secrets, the local filesystem and external access, then `lock_configuration`. Bridges attach, read, write and detach as before. Refused inside an explicit transaction, when the configuration is already locked, on NULL (including a NULL list element), when a name matches no system function, or when evaluated for more than one row |

### Policy validation

`bridge_validate_policy_config(source_catalog, policy)` — run on the source. One
row per setting that would make `bridge_configure_policy` or `ATTACH` fail; no
rows when there is none. Every setting is checked, not just the first that fails.

| Column | Type |
|---|---|
| `path` | VARCHAR — the setting, as in `bridge_configure_policy` errors; NULL for the document as a whole |
| `severity` | VARCHAR — `error`; `unchecked` for a predicate that uses `$context`, which is parsed but not bound; `data_dependent` for a `primary_key` whose `columns_from_query` or `verify_unique` failed against the data as it is now |
| `message` | VARCHAR |

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
