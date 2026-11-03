# duckdb-virtual-catalog

Two DuckDB extensions:

- **`vcatbridge`** — share tables with another DuckDB database, with permissions.
- **`vcatprovider`** — make tables from Python functions.

## Install

Requires DuckDB 1.5.6. The extensions aren't signed by DuckDB, so DuckDB must start with unsigned extensions allowed (`duckdb -unsigned`, or
`duckdb.connect(config={"allow_unsigned_extensions": "true"})` in Python).

```sql
INSTALL vcatbridge FROM 'https://storage.googleapis.com/n6k-duckdb-release';
LOAD vcatbridge;
```

Pin a release with `VERSION`: `'v0.1.0'` exactly, `'v0.1'` for the latest `0.1.x`, or `'v0'`
for the latest `0.x`:

```sql
INSTALL vcatbridge FROM 'https://storage.googleapis.com/n6k-duckdb-release' VERSION 'v0.1';
```

`vcatprovider` installs the same way.

## Quick start

- [Bridge](docs/python-quickstart-bridge.md) — share a table, filtered to one tenant's rows
- [Bridge over Quack](docs/python-quickstart-quack.md) — serve the shared tables over the network
- [Provider](docs/python-quickstart-provider.md) — make a table from Python

## Why

- Give untrusted code — a plugin, a notebook, an LLM agent — only the tables and rows it should see.
- One bridge per tenant, each filtered to that tenant's rows.
- Put the same permissions in front of SQLite or Postgres, attached through DuckDB.

## How it works

- **Source** — the DuckDB database that has the data. It decides what to share.
- **Target** — a separate DuckDB database in the same process. It `ATTACH`es the bridge and
  queries the shared tables as if they were its own.

### Setup

```
SOURCE                                         TARGET
──────                                         ──────
LOAD vcatbridge;                               LOAD vcatbridge;

SELECT vcatbridge_register_source('b1', 'db');
  → <token> ────────────────────────────────┐
SELECT vcatbridge_set_policy('b1', 'main.users',│
                         'select', true);   │
                                            └──▶ ATTACH '' AS app (TYPE vcatbridge,
                                                                   ID 'b1', TOKEN '<token>');

SELECT * FROM db.main.users;                     SELECT * FROM app.main.users;
```

Only the source can change the policy. Ungranted tables do not exist on the target.

### Pushdown

The bridge is built on [crossing](https://github.com/n6k-io/duckdb-crossing): it pushes as much of each query as it
safely can to the source — joins, aggregates, filters, functions — and runs the rest on the
target. Each table's row filter is applied on the source. The query crosses as a plan, never as
SQL text.

The examples below use this policy on the source, then the same `ATTACH ... AS app` as above:

```sql
SELECT vcatbridge_set_context('b1', 'tenant', 42);
SELECT vcatbridge_set_policy('b1', 'main.users', 'select', 'tenant_id = $tenant');
SELECT vcatbridge_set_policy('b1', 'main.orders', 'select', true);
SELECT vcatbridge_set_policy('b1', ':functions', 'whitelist', 'remove', 'upper');
```

**A join and aggregate on one bridge** — all on the source (`EXPLAIN`, abridged):

```sql
EXPLAIN SELECT u.name, sum(o.amount)
FROM app.main.users u JOIN app.main.orders o ON o.user_id = u.id
GROUP BY u.name;
```

```
CROSSING_READ                      ← everything below runs on the source
└─ PROJECTION   name, sum(amount)
   └─ AGGREGATE  group by name
      └─ JOIN    id = user_id
         ├─ users    (row filter: tenant_id = $tenant)
         └─ orders
```

A function that isn't whitelisted — `upper` above, or a UDF defined on the target — runs on the
target, and the rest of the query still crosses: target code never runs on the source.

**A join across two bridges** — two sources, one with `users` and one with `orders`, attached
on the target as `crm` and `shop`:

```sql
ATTACH '' AS crm  (TYPE vcatbridge, ID 'b1', TOKEN '<token1>');
ATTACH '' AS shop (TYPE vcatbridge, ID 'b2', TOKEN '<token2>');
```

Each source gets its own fragment, with the `WHERE` pushed into the source it belongs to. The
join runs on the target:

```sql
EXPLAIN SELECT u.name, sum(o.amount)
FROM crm.main.users u JOIN shop.main.orders o ON o.user_id = u.id
WHERE o.amount > 5
GROUP BY u.name;
```

```
HASH_GROUP_BY  group by name          ← target
└─ HASH_JOIN   id = user_id           ← target
   ├─ CROSSING_READ                   ← crm source
   │  └─ PROJECTION  id, name
   │     └─ users
   └─ CROSSING_READ                   ← shop source
      └─ FILTER      amount > 5
         └─ PROJECTION  user_id, amount
            └─ orders
```

## Docs

- [docs/sql-api.md](docs/sql-api.md) — every function
- [docs/vcatbridge.md](docs/vcatbridge.md) — the bridge model: setup,
  policy, row filters, primary keys, transactions
- [docs/vcatprovider.md](docs/vcatprovider.md) — the provider model and
  UDF contract
- [CHANGELOG.md](CHANGELOG.md)

## Building

Prerequisites: CMake and a C++14 compiler. Submodules must be checked out:

```sh
git submodule update --init --recursive
make release
```

This builds both extensions under `build/release/extension/`.
