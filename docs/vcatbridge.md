# vcatbridge

In-process DuckDB storage extension: a catalog whose contents come from another
`DatabaseInstance` in the same process, rather than from a file. A **source**
instance exposes selected tables to a **target** instance with per-table,
per-verb grants and row policies.

No network, no external server. Pure SQL API, usable from any DuckDB binding
(C++, Python, Go, Node, etc.).

Its sibling, [`virtual_catalog_provider`](virtual-catalog-provider.md), backs
tables with host-registered UDFs instead. The two are separate extensions with
separate catalog types and no shared SQL; either can be loaded without the
other, and a single attached catalog is one or the other, never both.

## Setup flow

Token-based handshake, all plain SQL statements; the target uses a different
connection than the source. The source grants first, then the target's `ATTACH`
redeems the token — one ATTACH is one bridge.

The target schemas do not have to exist: the attach creates one per granted
source schema, named the same.

```sql
-- 1. On the SOURCE: register, receive a one-time token.
SELECT bridge_register_source('b1', 'source_catalog');
-- → 'NONCE:UUID'

-- 2. On the SOURCE: set any context values the policy references.
SELECT bridge_set_context('b1', 'tenant', 42);

-- 3. On the SOURCE: set the policy, as one document...
SELECT bridge_configure_policy('b1', '{
  "policy_version": 1,
  "tables": {
    "main.users":   {"select": true, "update": "tenant_id = $tenant"},
    "main.logs":    {"select": true, "primary_key": ["id", "timestamp"]},
    "sales.orders": {"select": true}
  }
}');
-- ...or a setting at a time.
SELECT bridge_set_policy('b1', 'main.users', 'select', true);
-- → 'ok'

-- 4. On the TARGET: redeem the token into a new catalog.
ATTACH '' AS my_bridge (TYPE vcatbridge, ID 'b1', TOKEN 'NONCE:UUID');
-- my_bridge.main.users, my_bridge.main.logs, my_bridge.sales.orders
```

`ID` and `TOKEN` are both required. DuckDB folds every `ATTACH` option to a
constant at bind time, so neither can be a `?` parameter or a subquery — a host
formats the token into the statement text.

To open the bridge as the target's main database, open it with no path and
redeem the token with `vcatbridge_init`:

```python
target = duckdb.connect("vcatbridge:", config={"allow_unsigned_extensions": "true"})
target.execute("SELECT vcatbridge_init($1, $2)", ["b1", token])
```

The extension then loads by name as the database opens, so it must already be
installed, and the source must load that same installed copy. Until init, the
main database refuses every lookup; init runs once, and not after
`virtual_catalog_lockdown`. Quack clients see the main database on
`ATTACH 'quack:...'`.

The token contains a nonce unique to the loaded extension binary — mismatched
extension versions are rejected. Tokens expire after **30 seconds** if not
consumed — the clock measures inactivity, so it covers the whole policy script
rather than the first call alone. The policy is declared by the source (trusted
side) and cannot be modified by the target: every policy and context function
refuses any connection whose `DatabaseInstance` is not the one that called
`bridge_register_source` — so any connection on the source instance may change
the policy, but no connection outside it can. Once the token is redeemed the
policy stays open, like the [context](#context-values): `bridge_configure_policy`
and `bridge_set_policy` still work, and the target's next statement — inside an
open transaction too — sees the change. While a target transaction has DDL
through the bridge that it has not committed, table and schema policy changes are
refused (`:functions` whitelist changes are not); retry after it ends. `bridge_finalize_policy(bridge_id)` freezes the policy from that
call on, before or after `ATTACH`.

A source that already holds a secret can name the token itself, with the 3-arg
form. This is also what lets a test hardcode one:

```sql
SELECT bridge_register_source('b1', 'source_catalog', 'my-secret');
ATTACH '' AS my_bridge (TYPE vcatbridge, ID 'b1', TOKEN 'my-secret');
```

The source catalog may be another `vcatbridge`, so bridges chain:
each hop applies its own policies and context, and a write or DDL passes through
every hop. A `primary_key` set on one hop is not seen by the next; set it again
there (a native primary key is). The source may not be
`temp` or `system`.

A read streams through every hop when the statement runs in autocommit. Inside an
explicit transaction every hop, a single bridge included, holds its whole result
in memory: the transaction's reads share one source connection, and a second
query on it would close a streaming one.

The source catalog is pinned at registration by identity, not by name. A rename
(`ALTER DATABASE ... SET ALIAS TO ...`) is followed; after a `DETACH` of the source catalog
every read, write, DDL or grant through the bridge fails with
`source catalog '...' was detached`, even if another database is later attached
under the same name. This is also why the registered sources cannot form a
cycle: a bridge can only be registered on a catalog that already exists, and
never re-points. A source view or macro that names a bridge is not covered by
this.

## Teardown

```sql
-- On the TARGET:
DETACH my_bridge;
```

Not optional in a long-lived process when source and target are separate
instances. The catalog then holds a reference to the **source**
`DatabaseInstance`, so until the detach it is never released. When both are one
instance the catalog holds none, and closing the instance is enough.
Everything the catalog held goes with it, including any native entries created
alongside the bridged ones. A source registered but never attached holds its
instance until a later bridge call expires it.

Every refusal during `ATTACH` — a missing option, a bad token, a grant set that
does not hold together — is raised before the pending source is taken, so the
same token still redeems once the cause is fixed.

After the detach the bridge id no longer reaches the context.

## Policy

The policy says which tables the target sees, which verbs it may run on each,
and which rows each verb may reach and write. Absence is denial: a verb that is
not granted is refused, and a table with no granted verb is not in the catalog
(`CatalogException`).

Two functions set it. They take the same settings under the same names.

- `bridge_configure_policy(bridge_id, json)` replaces the whole policy with one
  document. The document is checked in full — against the JSON rules and against
  the source — before anything changes, so a failing document leaves the
  installed policy as it was. Order inside the document never matters.
- `bridge_set_policy(bridge_id, target, setting, value)` changes one setting.
  Assigning a setting overwrites it; `NULL` removes it. List settings take an
  operation instead: `bridge_set_policy(bridge_id, target, setting, op, value)`
  with `op` one of `add`, `remove`, `set`.

`bridge_validate_policy_config(source_catalog, json)` checks a document against
the source with no bridge registered, and returns a row per failing setting; see
[SQL API](sql-api.md#policy-validation).

Both work before and after `ATTACH`, until `bridge_finalize_policy`. Before
`ATTACH` the policy may be built a setting at a time in any order; `ATTACH`
checks that it holds together. After `ATTACH`, every change must leave a policy
that holds together on its own.

### The document

```json
{
  "$schema": "https://github.com/n6k-io/duckdb-virtual-catalog/docs/policy-v1.schema.json",
  "policy_version": 1,
  "schemas": {
    "main": {"create": true}
  },
  "tables": {
    "main.users": {
      "primary_key": ["id"],
      "select": "tenant = $tenant",
      "insert": "tenant = $tenant",
      "update": {"using": "tenant = $tenant", "check": "tenant = $tenant"},
      "delete": true,
      "foreign_keys": ["main.orgs"]
    },
    "main.orgs": {
      "select": "id IN (SELECT org_id FROM main.users WHERE tenant = $tenant)"
    }
  },
  "functions": {"add": ["my_udf"], "remove": ["random"]}
}
```

- `policy_version` is required; this extension reads version `1`.
  [`policy-v1.schema.json`](policy-v1.schema.json) describes the format, and an
  editor that reads `$schema` checks a document as you write it.
- An unknown key is an error, so a misspelt setting never silently grants or
  denies. So is a key given twice, a `null`, and a table entry that grants no
  verb.
- Errors name where they are: `tables."main.users".update.check: ...`.

### The setter

```sql
-- assign: the value replaces what was there
SELECT bridge_set_policy('b1', 'main.users', 'select', 'tenant = $tenant');
SELECT bridge_set_policy('b1', 'main.users', 'delete', true);
SELECT bridge_set_policy('b1', 'main.users', 'update', {'using': 'tenant = $tenant', 'check': 'tenant = $tenant'});
SELECT bridge_set_policy('b1', 'main.users', 'primary_key', ['id']);
SELECT bridge_set_policy('b1', 'main', 'create', true);

-- remove
SELECT bridge_set_policy('b1', 'main.users', 'delete', false);
SELECT bridge_set_policy('b1', 'main.users', 'primary_key', NULL);

-- list settings: add, remove or set
SELECT bridge_set_policy('b1', 'main.users', 'foreign_keys', 'add', 'main.orgs');
SELECT bridge_set_policy('b1', 'main.users', 'foreign_keys', 'set', ['main.orgs', 'main.teams']);
SELECT bridge_set_policy('b1', ':functions', 'whitelist', 'add', 'my_udf');
```

`target` is a table (`schema.table`), a schema for `create`, or `:functions`
for the function whitelist. `value` keeps its SQL type: a string, a boolean, a
list or a struct. `true` and `'true'` mean the same. Revoking a table's last
verb removes the table and its other settings.

### Names

A table is `schema.table` or `catalog.schema.table`, in SQL identifier syntax:
`"my.schema".t` names table `t` in schema `my.schema`. A catalog, when given,
must be the bridge's source catalog. A schema is its name alone.

Each granted source schema lands in the target schema of the same name, created
by setup if absent.

```sql
-- exposes shop.main.orders as b1_target.main.orders
SELECT bridge_register_source('b1', 'shop');
SELECT bridge_set_policy('b1', 'main.orders', 'select', true);
```

One bridge may span any number of source schemas. A catalog holds any number of
bridges, and two may serve the same schema, but not the same `(schema, table)`:
setup refuses that, naming the table and the bridge already serving it.

### Verbs

| Verb | Value | Grants |
|---|---|---|
| `select` | `true` or a predicate | reading the table; the predicate is which rows it reads |
| `insert` | `true`, a predicate, or `{"check": ...}` | `INSERT`; the predicate is the check every inserted row must pass |
| `update` | `true`, a predicate, or `{"using": ..., "check": ...}` | `UPDATE`; a predicate is both which rows it may reach and the check on what it writes |
| `delete` | `true` or a predicate | `DELETE`; the predicate is which rows it may reach |
| `alter` | `true` or a predicate over `$context` only | `ALTER TABLE`, when the `select` policy shows every row |
| `drop` | `true` | `DROP TABLE` |
| `create` | `true`, under `schemas` | `CREATE TABLE` in that schema |

`false`, or leaving the verb out, denies it.

A grant may name a source **view** as well as a table. A view takes `select`
only and its predicate is compiled against the view's columns. On the target it
is a `crossing` table entry with `verbs` `[select]` and no primary key.

`update` and `delete` can be granted without `select`. An `UPDATE` or `DELETE`
that reads the rows it writes — see [Row policies](#row-policies) — is then
refused.

DDL access can reveal ungranted table names; see
[SECURITY.md](../SECURITY.md#accepted-leaks).

The grant is checked once, where the statement's vcat node is created — the scan
for a read, the write fence for a write. That node then carries the check, so
nothing below it asks again.

### Row policies

A predicate has the two halves of a Postgres `CREATE POLICY`:

| Half | Clause | Constrains |
|---|---|---|
| `using` | `USING (...)` | which rows the verb may **reach** |
| `check` | `WITH CHECK (...)` | what the verb may **write** |

`true` grants unrestricted. Each predicate is compiled when it is set, against
the source table it applies to: it must be a single BOOLEAN expression over that
table's columns. Predicates are stored parsed and grafted into the source
statement as syntax-tree nodes, never spliced in as text.

- **`insert` has only a check.** Rows are appended through DuckDB's `Appender`,
  which has no `WHERE` to restrict, so the check is the only limit on what an
  insert may write.
- **`update` checks what it reached.** A plain predicate is the USING half, and
  the check defaults to it, as in Postgres — the rows an update may reach are the
  rows it may produce, so it cannot move a row out of the policy that permitted
  it. The check is evaluated against the row as it will be *after* the update.
  Give `{"using": ..., "check": true}` to opt out.
- **A check that evaluates to NULL is a violation**, not a pass.
- **A verb's policy governs that verb; `select` governs reading.** An `update` or
  `delete` reaches the rows its own policy allows. When the statement also reads
  those rows — a `WHERE`, a `RETURNING`, or a `SET` over a column — they must pass
  the `select` policy too, as in Postgres. Both are evaluated on the source's own
  rows, so a key that collates equal to a hidden one does not reach it.
  `DELETE FROM t` and `UPDATE t SET n = 7` read nothing: under an `update` or
  `delete` of `true` they reach rows the `select` policy hides. An `UPDATE` that
  reads nothing works its `SET` values out once on the target, so a volatile value
  such as `random()` is the same for every row.
- **A check constrains the bridge, not the source.** It is enforced in the plan
  the bridge builds. Anything else writing to the source is unaffected; that is
  what a `CHECK` constraint or a trigger on the source is for.

A policied `UPDATE` or `DELETE` reports the number of rows the source actually
changed, which can be fewer than a native statement with the same `WHERE`. For
one that reads nothing, that count includes rows the `select` policy hides.

A predicate may reference other tables on the source (`id IN (SELECT ...)`) —
the source wrote it and can already read them — and may be non-deterministic
(`random() < 0.5`), which makes row visibility vary between scans.

### Context values

A predicate may reference `$name`, a value the source set on this bridge with
`bridge_set_context(bridge_id, name, value)`:

```sql
SELECT bridge_set_context('b1', 'tenant', 42);
SELECT bridge_set_policy('b1', 'main.orders', 'select', 'tenant_id = $tenant');
```

The predicate is stored with `$name` in it. Each target statement takes one
snapshot of the context and fills every `$name` in as a constant node, so all
tables in a statement see the same values.

- **Set before use.** A predicate naming an unset `$name` is refused, so call
  `bridge_set_context` before the policy that uses it.
- **Live.** The context stays changeable after `ATTACH`, like the policy:
  `bridge_set_context` overwrites a value and the target's next statement —
  a prepared one included — uses it.
- **Checked on change.** A new value is bound into every predicate that uses it;
  one that stops a predicate binding or being BOOLEAN is refused and the old value
  stays. Binding casts nothing, so a value that only fails a cast (`'abc'`
  against an INTEGER column) passes, and the target's statement fails instead.
- **Lockable.** `bridge_finalize_context(bridge_id)` freezes the context, before
  or after `ATTACH`; every later `bridge_set_context` is refused. It is optional.
- **Any type, never NULL.** The value keeps its type (`42` is an INTEGER,
  `DATE '2024-01-01'` a DATE).
- **Named only.** Positional `$1` and `?` are refused.
- **Source only.** The target cannot set or read context. A `$name` in a target
  query is that query's own parameter and never reaches a policy.

### Foreign keys

The source checks a foreign key against every parent row, including rows the
target cannot see. An `insert` or `update` on a table with a foreign key
therefore succeeds or fails depending on whether the parent key exists.

`insert` and `update` on such a table are refused unless, for each parent, either:

- the parent is granted `select` with `true`, so the target can already read
  every parent key, or
- the owner accepts the leak by listing the parent in the table's
  `foreign_keys`.

```json
{"policy_version": 1, "tables": {
  "main.orgs":    {"select": "id > 0"},
  "main.members": {"select": true, "insert": true, "foreign_keys": ["main.orgs"]}
}}
```

A table that references itself is its own parent. A parent listed in
`foreign_keys` must be one the table really references, in the same schema
(DuckDB has no foreign keys across schemas).

### Functions

A function call in a target query crosses to the source only when it is on the
bridge's whitelist; any other call runs on the target over the fetched rows. The
whitelist starts as the built-ins that answer the same on either side.

- `"functions": {"add": [...], "remove": [...]}` changes that default. `remove`
  works on built-ins too.
- `"functions": [...]` sets the whitelist exactly; the built-ins are dropped,
  including function-backed operators (`+`, `*`, `~~` …). Comparisons, AND/OR,
  casts and CASE are not function calls and still push.

The setter spells these `bridge_set_policy(id, ':functions', 'whitelist', op,
names)`. A change is live on the target's next statement.

## Primary keys

Tables granted `update` or `delete` require a primary key. The bridge discovers
one by reading the `PRIMARY KEY` constraint off the source's own catalog entry —
the only mechanism that carries no assumption about the source's catalog shape.
Discovery runs for every verb, so `bridge_table_permissions` reports the key for
read-only tables too — but only `update` and `delete` make an empty result an
error, since only they resolve rows back to the source by key.

Where discovery cannot find one — a table whose catalog entry declares no
constraints — set `primary_key`. It overrides discovery; removing it brings the
discovered key back.

```json
{"policy_version": 1, "tables": {
  "main.events": {"select": true, "delete": true, "primary_key": ["tenant_id", "event_id"]}
}}
```

Or have the source answer the question in its own dialect. `columns_from_query`
runs your SQL on the source connection and reads column 0 of every row as a key
column, in key order:

```sql
SELECT bridge_set_policy('b1', 'main.orders', 'primary_key', {
  'columns_from_query': $$SELECT mysql_query('mydb', 'SHOW KEYS FROM orders WHERE Key_name = ''PRIMARY''')$$
});
```

### A declared key is a claim, and it is checked

A `primary_key` you set asserts a key; nothing has proven it. A key that is not
unique addresses a *group* of source rows, which turns a per-row `UPDATE` or
`DELETE` into a per-group one and reaches rows its `WHERE` did not match.

Two things guard against that.

`verify_unique` runs a query you write — again in the source's own dialect —
where zero rows means the key holds and any row rejects the setting:

```json
{"policy_version": 1, "tables": {
  "main.orders": {
    "select": true,
    "update": true,
    "primary_key": {
      "columns": ["order_id"],
      "verify_unique": "SELECT order_id FROM orders GROUP BY order_id HAVING count(*) > 1"
    }
  }
}}
```

A key that was not verified is reported to the target as not unique, and a
keyed write over it is planned for that.

`columns_from_query` and `verify_unique` run the SQL as written on the source,
before the rest of the policy is checked; anything that SQL does is not undone
when a later setting fails. The policy is the owner's own input.

Independently of any of that, every keyed write is checked as it runs: if the
source reports more affected rows than the statement sent key values, the write
is refused and rolled back.

```
virtual_catalog: UPDATE on 'orders' matched 3 source rows for 1 key value(s); the
declared primary key (grp) is not unique on the source. No changes were applied.
```

The message never names the offending value, since a permitted write is already
an existence oracle over hidden rows and this must not widen it. Note the guard
is one-sided: a key column containing NULL matches nothing rather than too much,
so such a write is a silent no-op reporting 0 rows.

## Reading

Every granted table is an ordinary table entry in the target catalog, whatever
its grants — read it by name:

```sql
SELECT * FROM my_bridge.main.users;
```

Grants decide the verbs, never the entry kind, so a `select`-only table still
answers `duckdb_tables()`, `DESCRIBE` and `SHOW ALL TABLES` the way a native
table does, and a write to it fails with a permission error.

Pushdown into the source:

- **Projection** — only requested columns are fetched from source
- **Filters** — `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`, `is_null`, `is_not_null`,
  and `AND`/`OR` conjunctions of those
- **Function calls** — only names on the bridge's [whitelist](#functions). A
  call that does not cross runs on the target over the fetched rows

A filter that cannot be rendered exactly makes the scan throw rather than fall
back to filtering locally.

Internally this builds a `Relation` on the source — parse-tree nodes carrying the
projection and WHERE clause. Nothing is assembled as SQL text; `make check-no-sql`
enforces that in CI.

## Writing

Tables granted any write verb support the ones they were granted.

**INSERT** uses DuckDB's `Appender` API on the source:

1. Incoming data chunks are accumulated during the sink phase
2. On finalize, an `Appender` writes all chunks to the source table
3. The appender commits on the source immediately

**UPDATE** and **DELETE** are keyed on the primary key, not on any source rowid:

1. The scan appends each row's primary-key values to a shared key buffer
   and emits the buffer index in the `rowid` slot
2. Those indices are collected during the sink phase
3. On finalize, prepared statements on the source match rows by the primary-key
   values the indices resolve to

The grant is checked once, when the write fence is built at bind time. The fence
carries the proof, along with the key its seam is named from, so physical
planning and execution read it rather than consulting the grant map again:
grants can change after bind, and a second lookup could disagree with the
first.

**Not yet supported:** `ON CONFLICT`, and the `RETURNING` clause on
INSERT/UPDATE/DELETE.

## Introspecting grants

`bridge_table_permissions` reports, per table/view in an attached catalog, which
statements it accepts. It works on a `vcatbridge` catalog and on a
plain DuckDB catalog. Signature and columns: [`sql-api.md`](sql-api.md).

`kind` is one of `native_table` · `native_view` · `crossing`. Capability by entry
kind:

| `kind` | `verbs` | `primary_key` |
|--------|---------|---------------|
| `native_table` | all five | from table constraints |
| `native_view` | `select` | empty |
| `crossing` | the granted verbs | discovered from source |

A `virtual_catalog_provider` catalog reports its own entries as `provider`;
`provider_table_permissions` is the same function compiled into the provider.

`verbs` is the single source of truth for capability. There is deliberately no
coarse `writeable`/`editable` pair alongside it — those existed, duplicated a
subset of this list, and drifted from it. A `READ_ONLY` attach narrows `verbs`
to `select` whatever the entry's own type says, because the engine will refuse
the write regardless and the client should not learn that only on failure.

For a `vcatbridge` catalog it is a fast, in-memory metadata lookup:
it reads the bridge grant map and the native catalog type directly — no
source-DB query. On a plain DuckDB catalog it classifies native tables/views
from the catalog directly.

`kind` is an open string set: an extension layered on top of this one may report
values not listed above for catalog types it owns.

The optional `schema` / `"table"` named parameters are case-insensitive filters.

### Single-table descriptor: `bridge_table_describe`

Returns one fully-typed row for a single table. It reuses the same per-kind capability collectors as
`bridge_table_permissions` for the verbs and primary key, and reads columns
/ enum domains from the catalog entry — so a client gets everything it needs to
render a table in one call instead of stitching together `DESCRIBE`,
`duckdb_constraints()`, and per-column `enum_range()`.

## Transaction semantics

- Stateless — commit and rollback on the target are no-ops
- No snapshot isolation — target always sees the source's latest committed data
- Inserts commit on the source independently of the target transaction

## Unified catalog (`TYPE vcatbridge`)

The catalog type is unified in the sense that native DuckDB tables and
bridge-mirrored source tables coexist freely in the same schema:

```sql
ATTACH '' AS workspace (TYPE vcatbridge, ID 'b1', TOKEN 'NONCE:UUID');
```

`workspace` behaves exactly like a native in-memory DuckDB catalog — full
native storage and ACID transactions via DuckDB's own `DuckTransactionManager`.
On top of that, the same schema can host bridge entries. Operations route by
origin:

| Op on... | Native entry | Bridge entry |
|---|---|---|
| SELECT / scan | native | bridge source query |
| INSERT / UPDATE / DELETE | native | routed via phantom `BridgeCatalog` → bridge |
| ALTER TABLE | native | rejected |
| DROP TABLE | native | rejected — DETACH the catalog instead |
| CREATE TABLE / CREATE VIEW | native — forwarded to wrapped DuckSchemaEntry | n/a |

A name collision resolves native-first: the wrapped schema is consulted before
any bridge route, so a `CREATE TABLE` shadows a bridged table of the same name
rather than colliding with it.

The bridge arrives with the ATTACH; `CREATE TABLE` and `CREATE SCHEMA` follow it
in any order.

Provider entries cannot live here — they need `TYPE virtual_catalog_provider`,
a separate catalog from a separate extension.

## Using from other languages

Any DuckDB binding that can execute SQL can drive the handshake directly. The
statements in [Setup flow](#setup-flow) and [Teardown](#teardown) are the
complete API surface; there are no out-of-band calls.

Rough sketch in Go (pseudocode):

```go
// source connection
var token string
sourceDB.QueryRow(`
    SELECT bridge_register_source(?, ?)
`, bridgeID, sourceCatalog).Scan(&token)

// policy is a document as in "The document" above, e.g. marshalled from a struct.
sourceDB.Exec(`SELECT bridge_configure_policy(?, ?)`, bridgeID, policy)

// target connection. ATTACH options are folded to constants at bind time, so the id and the
// token go into the statement text rather than into placeholders.
targetDB.Exec(fmt.Sprintf(
    `ATTACH '' AS my_bridge (TYPE vcatbridge, ID '%s', TOKEN '%s')`, bridgeID, token))

// ... and on the way out, so the source DatabaseInstance is released
targetDB.Exec(`DETACH my_bridge`)
```

Identical code works from Node (duckdb-node), Rust (duckdb-rs), C++, Java, etc.

## See also

- [`sql-api.md`](sql-api.md) — every registered function, its arguments and columns.
