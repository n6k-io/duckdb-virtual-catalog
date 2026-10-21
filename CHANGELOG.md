# Unreleased

## virtual_catalog_bridge

- **Breaking:** two functions replace every policy function. `bridge_configure_policy(id, json)` replaces the whole policy with a JSON document ([schema](docs/policy-v1.schema.json)), checked in full before anything changes. `bridge_set_policy(id, target, setting, value)` assigns one setting; list settings take `add`, `remove` or `set`.

  | Removed | Now |
  |---|---|
  | `bridge_policy(id, t, verb, using)` | `bridge_set_policy(id, t, verb, using)` |
  | `bridge_policy(id, t, 'insert', 'true', check)` | `bridge_set_policy(id, t, 'insert', check)` |
  | `bridge_policy(id, t, 'update', using, check)` | `bridge_set_policy(id, t, 'update', {'using': using, 'check': check})` |
  | `bridge_primary_key(id, t, cols)` | `bridge_set_policy(id, t, 'primary_key', cols)` |
  | `bridge_primary_key_query(id, t, sql)` | `bridge_set_policy(id, t, 'primary_key', {'columns_from_query': sql})` |
  | `bridge_primary_key_check(id, t, sql)` | `bridge_set_policy(id, t, 'primary_key', {'columns': cols, 'verify_unique': sql})` |
  | `bridge_allow_foreign_key(id, t, parent)` | `bridge_set_policy(id, t, 'foreign_keys', 'add', parent)` |
  | `bridge_allow_function(id, f)` | `bridge_set_policy(id, ':functions', 'whitelist', 'add', f)` |
  | `bridge_disallow_function(id, f)` | `bridge_set_policy(id, ':functions', 'whitelist', 'remove', f)` |

- `bridge_validate_policy_config(source_catalog, json)` reports every setting in a policy document that would make `bridge_configure_policy` or `ATTACH` fail, without a bridge.
- Setting a verb again overwrites it instead of failing; `false` or `NULL` revokes it. Revoking a table's last verb removes the table.
- `insert` takes its check as its value; `true` means check `true`.
- The function whitelist can be set exactly (`'set'`, or `"functions": [...]`), dropping the built-ins.
- Removing a `primary_key` restores the discovered key.
- Table names may be `catalog.schema.table` when the catalog is the source catalog, and are parsed as SQL identifiers, so a name with a dot can be granted quoted.
- A table that sets a key or foreign keys but grants no verb fails `ATTACH` instead of being dropped.
- After `ATTACH`, every policy change is checked against the whole policy, and table and schema changes are refused while a target transaction has uncommitted DDL through the bridge.
- **Breaking:** an `update` or `delete` reaches the rows its own policy allows; the `select` policy also applies only when the statement reads those rows (`WHERE`, `RETURNING`, or a `SET` over a column), as in Postgres. `DELETE FROM t` and `UPDATE t SET n = 7` now reach rows the `select` policy hides when their own policy allows them.
- `update` and `delete` can be granted without `select`; a write that reads rows is then refused.
- An `UPDATE` or `DELETE` keyed on a collated column no longer reaches a hidden row whose key collates equal to a visible one.

# 2026 Oct, 12

## virtual_catalog_bridge

- Bridges chain: a source catalog may itself be a `virtual_catalog_bridge`. Each hop applies its own policies and context; reads, writes, DDL and rollback pass through every hop.
- A native primary key is discovered through every hop. A key declared with `bridge_primary_key` covers one hop; declare it again on the next.
- Reads stream through every hop in autocommit. Inside an explicit transaction every hop, a single bridge included, holds its whole result in memory.
- The source catalog is pinned at registration by identity, not by name. A rename (`ALTER DATABASE ... SET ALIAS TO ...`) is followed; after a `DETACH` of the source every read, write, DDL or grant fails with `source catalog '...' was detached`, even if another database is attached under the same name.
- Registered sources cannot form a cycle. A source view or macro that names a bridge is not covered.
- `temp` and `system` are refused as a source catalog.
- `insert` and `update` on a table with a foreign key are refused unless the parent is granted `select` with `'true'`, or `bridge_allow_foreign_key` accepts that the write reveals which parent keys exist.
- `bridge_policy` and `bridge_primary_key*` work after `ATTACH`; the target's next statement sees the change. `bridge_finalize_policy` freezes them, before or after `ATTACH`.
- A bridge whose source is on its own instance no longer keeps that instance alive; closing it is enough. Across instances `DETACH` is still required.
- Fixed: `UPDATE ... FROM` rows held on the target could write wrong values, update the wrong row, or update nothing.
- Fixed: a join the source runs, read by an expression only the target can compute, failed with `Failed to bind column reference` and invalidated the database.
- Fixed: reads never streamed; every result was materialized on the source first.

# 2026 Oct, 10

## virtual_catalog_bridge

- A grant can name a source view as well as a table. A view can only be granted `select`; `bridge_policy` refuses any other verb.

## virtual_catalog_provider

- A provider is scoped to the database it is attached in. The same catalog name in two databases no longer collides.
- Fixed: closing a database with a provider registered no longer keeps it open, so its file can be reopened.
