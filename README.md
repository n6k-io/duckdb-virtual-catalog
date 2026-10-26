# virtual_catalog

Two DuckDB extensions:

- **`virtual_catalog_bridge`** — share tables with another DuckDB database, with permissions.
- **`virtual_catalog_provider`** — make tables from Python functions.

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

Source and target are two `duckdb.connect()` calls in one program, each its own database — not
two files `ATTACH`ed to one connection.

### Setup

```
SOURCE                                         TARGET
──────                                         ──────
LOAD virtual_catalog_bridge;                     LOAD virtual_catalog_bridge;

SELECT bridge_register_source('b1', 'db');
  → <token> ────────────────────────────────┐
SELECT bridge_set_policy('b1', 'main.users',│
                         'select', true);   │
                                            └──▶ ATTACH '' AS app (TYPE virtual_catalog_bridge,
                                                                   ID 'b1', TOKEN '<token>');

SELECT * FROM db.main.users;                     SELECT * FROM app.main.users;
```

Only the source can change the policy. Ungranted tables do not exist on the target.

### Pushdown

The bridge is built on [crossing](third_party/crossing): it pushes as much of each query as it
safely can to the source — joins, aggregates, filters, functions — and runs the rest on the
target. Each table's row filter is applied on the source. The query crosses as a plan, never as
SQL text.

The examples below use this policy on the source, then the same `ATTACH ... AS app` as above:

```sql
SELECT bridge_set_context('b1', 'tenant', 42);
SELECT bridge_set_policy('b1', 'main.users', 'select', 'tenant_id = $tenant');
SELECT bridge_set_policy('b1', 'main.orders', 'select', true);
SELECT bridge_set_policy('b1', ':functions', 'whitelist', 'remove', 'upper');
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

**A function that can't cross** — `upper`, removed from the whitelist above. Only `upper` stays
on the target; the join and aggregate still cross:

```sql
EXPLAIN SELECT upper(u.name), sum(o.amount)
FROM app.main.users u JOIN app.main.orders o ON o.user_id = u.id
GROUP BY u.name;
```

```
PROJECTION   upper(name), sum(amount)   ← runs on the target
└─ CROSSING_READ                        ← same join + aggregate on the source
   └─ AGGREGATE  group by name
      └─ JOIN    id = user_id
         ├─ users    (row filter: tenant_id = $tenant)
         └─ orders
```

A UDF defined on the target stays on the target the same way: target code never runs on the
source.

**A join across two bridges** — two sources, one with `users` and one with `orders`, attached
on the target as `crm` and `shop`:

```sql
ATTACH '' AS crm  (TYPE virtual_catalog_bridge, ID 'b1', TOKEN '<token1>');
ATTACH '' AS shop (TYPE virtual_catalog_bridge, ID 'b2', TOKEN '<token2>');
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

## virtual_catalog_provider

Back a catalog with your own functions: you register `list`, `schema` and `scan` UDFs (plus
optional `insert`, `update`, `delete`), returning Arrow IPC, and DuckDB sees ordinary tables.
Useful for serving data from an API, an in-memory structure, or another engine.
See [docs/virtual-catalog-provider.md](docs/virtual-catalog-provider.md).

## Docs

- [docs/sql-api.md](docs/sql-api.md) — every function
- [docs/virtual-catalog-bridge.md](docs/virtual-catalog-bridge.md) — the bridge model: setup,
  policy, row filters, primary keys, transactions
- [docs/virtual-catalog-provider.md](docs/virtual-catalog-provider.md) — the provider model and
  UDF contract
- [docs/policy-v1.schema.json](docs/policy-v1.schema.json) — policy document schema
- [CHANGELOG.md](CHANGELOG.md)

## Building

Prerequisites: CMake and a C++14 compiler. Submodules must be checked out:

```sh
git submodule update --init --recursive
make release
```

This builds both extensions under `build/release/extension/`.

## Testing

```sh
make test                  # sqllogictests under test/sql/
make test-cpp              # C++ unit tests
uv run pytest test/python/ # cross-instance tests (needs make release)
```

## Formatting

```sh
make format-fix    # clang-format (make format-check to verify)
make tidy-check    # clang-tidy
```

Both need `black` and the clang tools on PATH.
