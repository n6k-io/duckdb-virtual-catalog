# bridge_set_policy / bridge_configure_policy — discussion notes

Status: discussion only, nothing implemented.

## Current surface

`bridge_register_source`, `bridge_policy`, `bridge_primary_key`, `bridge_primary_key_query`,
`bridge_primary_key_check`, `bridge_allow_function`, `bridge_disallow_function`,
`bridge_set_context`, `bridge_finalize_context`, `bridge_finalize_policy`.

Each new policy option has added a function, and ordering already matters (keys before grants, see
`test/python/conftest.py:119`).

## Proposed shape

Two entry points:

- `bridge_set_policy` — key/value setter, one setting per call.
- `bridge_configure_policy` — whole policy as one JSON document, validated atomically.

```sql
SELECT bridge_set_policy(:id, 'main.users', 'select', 'tenant = 1');
SELECT bridge_set_policy(:id, 'main.users', 'key', ['id']);
SELECT bridge_set_policy(:id, 'main.users', 'allow_foreign_key', 'main.secret_orgs');
SELECT bridge_set_policy(:id, ':allow_function', 'my_udf');
```

```sql
SELECT bridge_configure_policy(:id, '{
  "tables": {
    "main.users": {
      "key": ["id"],
      "select": "tenant = 1",
      "insert": {"check": "tenant = 1"},
      "update": "tenant = 1",
      "allow_foreign_keys": ["main.secret_orgs"]
    }
  },
  "functions": {"allow": ["my_udf"]}
}');
```

## Non-table scopes

Non-table settings use a `:` sigil (`':allow_function'`). An unquoted `:name` is not a valid SQL
identifier, so it cannot be a table if the table argument is parsed as identifiers. A table literally
named `:allow_function` is written `'main.":allow_function"'`.

Today `SplitGrantName` (`src/bridge/duckdb_source.cpp:170`) splits on `.` with no quote handling:

- a bare `:name` has no dot, so it is already never a table;
- tables/schemas containing `.` cannot be granted, and quotes are kept literally.

Switching to real identifier parsing (`QualifiedName::Parse`) would fix both.

## n6kd convergence

`n6kd/examples/perms.yml` has nearly the same shape (`tables:` → `key`, `select`, `insert`, ...).
`n6kd/engine/session.go` currently translates it into `bridge_primary_key` / `bridge_policy` calls.
If `bridge_configure_policy` accepts the `perms.yml` schema as JSON, n6kd can pass it through.
Differences to reconcile: `perms.yml` names are `catalog.schema.table`; bridge functions take
`schema.table`.

## Open

- Whether the new functions replace or sit on top of the existing ones.
