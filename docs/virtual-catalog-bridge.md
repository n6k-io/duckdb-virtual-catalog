# virtual_catalog_bridge

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

-- 2. On the SOURCE: grant one (table, verb) at a time. Names are 'schema.table'.
--    Optionally supply a key first.
SELECT bridge_primary_key('b1', 'main.logs', ['id', 'timestamp']);
SELECT bridge_policy('b1', 'main.users',  'select', 'true');
SELECT bridge_policy('b1', 'main.users',  'update', 'tenant_id = 42');
SELECT bridge_policy('b1', 'sales.orders', 'select', 'true');
-- → 'ok'

-- 3. On the TARGET: redeem the token into a new catalog.
ATTACH '' AS my_bridge (TYPE virtual_catalog_bridge, ID 'b1', TOKEN 'NONCE:UUID');
-- my_bridge.main.users, my_bridge.main.logs, my_bridge.sales.orders
```

`ID` and `TOKEN` are both required. DuckDB folds every `ATTACH` option to a
constant at bind time, so neither can be a `?` parameter or a subquery — a host
formats the token into the statement text.

The token contains a nonce unique to the loaded extension binary — mismatched
extension versions are rejected. Tokens expire after **30 seconds** if not
consumed — the clock measures inactivity, so it covers the whole grant script
rather than the first call alone. Grants are declared by the source (trusted
side) and cannot be modified by the target: `bridge_policy` refuses any connection
whose `DatabaseInstance` is not the one that called `bridge_register_source` — so
any connection on the source instance may add grants, but no connection outside
it can. Once the token is redeemed the grant set is frozen.

A source that already holds a secret can name the token itself, with the 3-arg
form. This is also what lets a test hardcode one:

```sql
SELECT bridge_register_source('b1', 'source_catalog', 'my-secret');
ATTACH '' AS my_bridge (TYPE virtual_catalog_bridge, ID 'b1', TOKEN 'my-secret');
```

The source catalog may not itself be a `virtual_catalog_bridge` — bridges do not
chain.

## Teardown

```sql
-- On the TARGET:
DETACH my_bridge;
```

Not optional in a long-lived process. The catalog holds a reference to the
**source** `DatabaseInstance`, so until the detach it is never released.
Everything the catalog held goes with it, including any native entries created
alongside the bridged ones.

Every refusal during `ATTACH` — a missing option, a bad token, a grant set that
does not hold together — is raised before the pending source is taken, so the
same token still redeems once the cause is fixed.

## Permission model

One `bridge_policy(bridge_id, table, verb, policy_using[, policy_check])` call
grants one verb on one table. Absence is denial: a verb with no call is refused,
and a table with no call at all is not in the catalog (`CatalogException`). The
call is not idempotent: granting the same `(table, verb)` twice is an error.
`table` is `schema.table`.

The grant is checked once, where the statement's vcat node is created — the scan
for a read, the write fence for a write. That node then carries the check, so
nothing below it asks again.

| Verb | Grants |
|------|--------|
| `select` | reading the table |
| `insert` | `INSERT` |
| `update` | `UPDATE` |
| `delete` | `DELETE` |
| `alter` | reported in `verbs`; bridge `ALTER TABLE` is not yet implemented |

`update` and `delete` also require `select` on the same table, and the `ATTACH`
refuses a grant set that breaks this. Both are driven by a
scan: the plan reads the table to resolve the `WHERE`, and the row identities the
write path uses are buffered by that scan.

### Row policies

A policy has the two halves of a Postgres `CREATE POLICY`:

| Argument | Clause | Constrains |
|---|---|---|
| `policy_using` (4th, mandatory) | `USING (...)` | which rows the verb may **reach** |
| `policy_check` (5th, optional) | `WITH CHECK (...)` | what the verb may **write** |

`'true'` grants unrestricted — spelled out, so no short form grants unrestricted
access by accident.

Both halves are compiled at grant time against the source table they will apply
to: each must be a single BOOLEAN expression over that table's columns, with no
parameters. They are stored parsed and grafted into the source statement as
syntax-tree nodes, never spliced in as text.

Per verb:

| Verb | `policy_using` | `policy_check` |
|------|----------------|----------------|
| `select` | any predicate | rejected — a select writes no row |
| `delete` | any predicate | rejected — a delete writes no row |
| `insert` | must be `'true'` | **required** |
| `update` | any predicate | defaults to `policy_using` |
| `alter` | must be `'true'` | rejected |

Things worth knowing:

- **`insert` and `alter` USING predicates must be literally `'true'`.** Rows are
  appended through DuckDB's `Appender`, which has no `WHERE` to restrict, and an
  `ALTER` touches no rows at all. A predicate that cannot be enforced is refused
  rather than ignored.
- **`insert` must state its check.** Because its USING predicate can restrict
  nothing, the check is the only limit on what an insert may write, and there is
  no USING to inherit one from. Pass `'true'` to declare it deliberately
  unrestricted; the attach warns on the source's log when you do.
- **`update` inherits.** An omitted `policy_check` is the USING predicate, as in
  Postgres — the rows an update may reach are the rows it may produce, so it
  cannot move a row out of the policy that permitted it. The check is evaluated
  against the row as it will be *after* the update. Pass `'true'` to opt out.
- **A check that evaluates to NULL is a violation**, not a pass.
- **Policies compose.** The rows an `update` or `delete` can reach are those
  passing *both* the `select` policy and its own — so an `update` policy of
  `'true'` under a restrictive `select` policy is not unrestricted.
- **A check constrains the bridge, not the source.** It is enforced in the plan
  the bridge builds. Anything else writing to the source is unaffected; that is
  what a `CHECK` constraint or a trigger on the source is for.

A policied `UPDATE` or `DELETE` reports the number of rows the source actually
changed, which can be fewer than a native statement with the same `WHERE`.

A predicate may reference other tables on the source (`id IN (SELECT ...)`) —
the source wrote it and can already read them — and may be non-deterministic
(`random() < 0.5`), which makes row visibility vary between scans.

## Primary keys

Tables granted `update` or `delete` require a primary key. The bridge discovers
one on the first grant for a table, by reading the `PRIMARY KEY` constraint off
the source's own catalog entry — the only mechanism that carries no assumption
about the source's catalog shape. Discovery runs for every verb, so
`bridge_table_permissions` reports the key for read-only tables too — but only
`update` and `delete` make an empty result an error, since only they resolve rows
back to the source by key.

Where discovery cannot find one — a view, a table function, or a scanner whose
catalog entry declares no constraints — supply it:

```sql
SELECT bridge_primary_key('b1', 'main.events', ['tenant_id', 'event_id']);
```

`bridge_primary_key` overrides whatever discovery found, so it may be called before
or after the grants — except for a table with no discoverable key at all, where
it must come first, since the `update`/`delete` grant is refused without one.

Or have the source answer the question in its own dialect. `bridge_primary_key_query`
runs your SQL on the source connection and reads column 0 of every row as a key
column, in key order:

```sql
SELECT bridge_primary_key_query('b1', 'main.orders',
  $$SELECT mysql_query('mydb', 'SHOW KEYS FROM orders WHERE Key_name = ''PRIMARY''')$$);
```

### A declared key is a claim, and it is checked

`bridge_primary_key` and `bridge_primary_key_query` assert a key; nothing has proven
it. A key that is not unique addresses a *group* of source rows, which turns a
per-row `UPDATE` or `DELETE` into a per-group one and reaches rows the `select`
policy hides.

Two things guard against that.

`bridge_primary_key_check` runs a validation query you write — again in the source's
own dialect — where zero rows means the key holds and any row rejects it:

```sql
SELECT bridge_primary_key_check('b1', 'main.orders',
  'SELECT order_id FROM orders GROUP BY order_id HAVING count(*) > 1');
```

It is optional. When a key was asserted and never checked, the attach
writes a warning to the **source's** log (the source is what made the claim; the
target cannot tell a proven key from a claimed one).

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

## Which source schemas a bridge exposes

Grant names are `schema.table`, resolved inside the catalog named in argument 2
of `bridge_register_source`. Each granted source schema lands in the target schema
of the same name, created by setup if absent.

```sql
-- exposes shop.main.orders as b1_target.main.orders
SELECT bridge_register_source('b1', 'shop');
SELECT bridge_policy('b1', 'main.orders', 'select', 'true');
```

A bare name or a three-part name is rejected — the source catalog is fixed at
registration, so there is nowhere for a third part to go. A source name
containing a literal dot cannot be granted.

One bridge may span any number of source schemas. A catalog holds any number of
bridges, and two may serve the same schema, but not the same `(schema, table)`:
setup refuses that, naming the table and the bridge already serving it.

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

1. The scan appends each row's primary-key values to a shared `BridgePKBuffer`
   and emits the buffer index in the `rowid` slot
2. Those indices are collected during the sink phase
3. On finalize, prepared statements on the source match rows by the primary-key
   values the indices resolve to

The grant is checked once, when the write fence is built at bind time. The fence
carries the proof, along with the key its seam is named from, so physical
planning and execution read it rather than consulting the grant map again —
grants are frozen at attach time, so a second lookup could only ever
disagree with the first.

**Not yet supported:** `CREATE TABLE` in a *bridge* schema (it works in a plain
`virtual_catalog_bridge` schema, as a native entry), `ON CONFLICT`, and the
`RETURNING` clause on INSERT/UPDATE/DELETE.

## Introspecting grants

`bridge_table_permissions` reports, per table/view in an attached catalog, which
statements it accepts. It works on a `virtual_catalog_bridge` catalog and on a
plain DuckDB catalog. Signature and columns: [`sql-api.md`](sql-api.md).

`kind` is one of `native_table` · `native_view` · `bridge`. Capability by entry
kind:

| `kind` | `verbs` | `primary_key` |
|--------|---------|---------------|
| `native_table` | all five | from table constraints |
| `native_view` | `select` | empty |
| `bridge` | the granted verbs (bridge `alter` is reported but not yet implemented, so it currently overstates what the target can do) | discovered from source |

A `virtual_catalog_provider` catalog is a different extension's binary, so this
function does not recognise it and falls back to the plain-catalog classifier.
Use `provider_table_permissions` for those; it is the same function compiled
into the provider.

`verbs` is the single source of truth for capability. There is deliberately no
coarse `writeable`/`editable` pair alongside it — those existed, duplicated a
subset of this list, and drifted from it. A `READ_ONLY` attach narrows `verbs`
to `select` whatever the entry's own type says, because the engine will refuse
the write regardless and the client should not learn that only on failure.

For a `virtual_catalog_bridge` catalog it is a fast, in-memory metadata lookup:
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

## Unified catalog (`TYPE virtual_catalog_bridge`)

The catalog type is unified in the sense that native DuckDB tables and
bridge-mirrored source tables coexist freely in the same schema:

```sql
ATTACH '' AS workspace (TYPE virtual_catalog_bridge, ID 'b1', TOKEN 'NONCE:UUID');
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

for _, g := range grants {
    // g.Table is "schema.table". g.Check may be nil for select/delete, and for update to
    // inherit g.Using; an insert grant must set it, to "true" if unrestricted.
    sourceDB.Exec(`SELECT bridge_policy(?, ?, ?, ?, ?)`, bridgeID, g.Table, g.Verb, g.Using, g.Check)
}

// target connection. ATTACH options are folded to constants at bind time, so the id and the
// token go into the statement text rather than into placeholders.
targetDB.Exec(fmt.Sprintf(
    `ATTACH '' AS my_bridge (TYPE virtual_catalog_bridge, ID '%s', TOKEN '%s')`, bridgeID, token))

// ... and on the way out, so the source DatabaseInstance is released
targetDB.Exec(`DETACH my_bridge`)
```

Identical code works from Node (duckdb-node), Rust (duckdb-rs), C++, Java, etc.

## See also

- [`sql-api.md`](sql-api.md) — every registered function, its arguments and columns.
- [`../ARCHITECTURE.md`](../ARCHITECTURE.md) — catalog architecture, write path,
  registries, and thread safety.
