# Per-row policies for Quack

Quack lets clients query a DuckDB database over the network, but it can't limit which tables or rows they see.

I wrote vcatbridge, a DuckDB extension that can. Here's a walkthrough with two DuckDB instances in one process: SourceDB has the data, and SharedDB is what you hand to someone else. A controller opens both and keeps SourceDB to itself.

**1. Create some data on SourceDB**

```sql
-- SourceDB
LOAD vcatbridge;

CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
CREATE TABLE secrets (k VARCHAR, v VARCHAR);
```

Two tenants in `users`, and a `secrets` table nobody else should see.

**2. Set up the bridge on SourceDB**

Register a bridge, say which tenant it's for, and grant `users` to that tenant. `secrets` gets no policy, and there's no `delete` on anything.

```sql
-- SourceDB
SELECT vcatbridge_register_source('b1', 'memory');   -- returns a token
SELECT vcatbridge_set_context('b1', 'tenant', 42);
SELECT vcatbridge_set_policy('b1', 'main.users', 'select', 'tenant_id = $tenant');
SELECT vcatbridge_set_policy('b1', 'main.users', 'insert', 'tenant_id = $tenant');
SELECT vcatbridge_set_policy('b1', 'main.users', 'update', 'tenant_id = $tenant');
```

Only SourceDB can change these. The policy and context functions refuse any instance other than the one that registered the bridge.

**3. Connect SharedDB and query**

The controller opens SharedDB as `vcatbridge:` and passes it the token:

```sql
-- SharedDB
SELECT vcatbridge_init('b1', '<token>');
```

SharedDB can now query SourceDB's tables, through the policy:

```sql
-- SharedDB
SELECT * FROM users;
-- (1, 42, 'ada'), (2, 42, 'grace')

INSERT INTO users VALUES (5, 7, 'mallory');
-- Invalid Input Error: vcatbridge: row violates the check predicate on this table

DELETE FROM users WHERE id = 2;
-- Permission Error: crossing: 'users' does not have 'delete' permission

FROM secrets;
-- Catalog Error: Table with name secrets does not exist!
```

`secrets` isn't hidden behind a permission error. It isn't in SharedDB's catalog, so there's nothing to name.

The query crosses to SourceDB as a plan, never as SQL text. Filters, joins and aggregates go with it, and the row filter is applied on SourceDB, under everything else. For a join of `users` with an `orders` table:

```sql
-- SharedDB
EXPLAIN SELECT name, sum(amount) FROM users JOIN orders ON id = user_id GROUP BY name;
```

```
╭─ Crossing Remote ─────────────────────────────────────────╮
│ Runs on source:                                           │
│ ╭─ Projection ──────────────╮,                            │
│ │ Expressions: name,        │,                            │
│ │              sum(amount)  │,                            │
│ ╰─────────────┬─────────────╯,                            │
│ ╭─ Aggregate ─┴─────────────╮,                            │
│ │ Groups: name              │,                            │
│ │ Expressions: sum(amount)  │,                            │
│ ╰─────────────┬─────────────╯,                            │
│ ╭─ Comparison Join ─────────╮,                            │
│ │ Join Type: INNER          │,                            │
│ │ Conditions: id = user_id  │,                            │
│ ╰─────────────┬─────────────╯,                            │
│               ├────────────────────────────╮,             │
│ ╭─ Projection ┴─────────────╮  ╭─ Projection ──────────╮, │
│ │ Expressions: id, name     │  │ Expressions: user_id, │, │
│ ╰─────────────┬─────────────╯  │              amount   │, │
│ ╭─ Crossing Floor ──────────╮  ╰───────────┬───────────╯, │
│ ╰───────────────────────────╯  ╭─ Crossing Floor ──────╮, │
│                                ╰───────────────────────╯  │
│ Returns: rows                                             │
╰───────────────────────────────────────────────────────────╯
```

A function crosses to SourceDB only if it's on the bridge's whitelist. Anything else, such as `current_setting`, runs on SharedDB.

**4. Serve SharedDB over Quack, and lock it down**

```sql
-- SharedDB
SET allow_persistent_secrets = false;
INSTALL quack;  LOAD quack;
INSTALL httpfs; LOAD httpfs;

CALL quack_serve('quack:localhost:9494', token = 'super_secret');
```

That already serves the policy. To also keep clients off the host, lock SharedDB down:

```sql
-- SharedDB
SELECT vcatbridge_lockdown('1GB', 2, ['quack_serve']);
```

Lockdown takes files, network, extensions and settings away from SharedDB, caps its memory and threads, and drops `quack_serve` so clients can't start servers.

**5. Query from any DuckDB**

```sql
INSTALL quack; LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:localhost:9494' AS app;

SELECT * FROM app.users;
-- (1, 42, 'ada'), (2, 42, 'grace')

FROM quack_query('quack:localhost:9494', 'SELECT * FROM read_csv(''/etc/passwd'')');
-- Permission Error: File system LocalFileSystem has been disabled by configuration
```

Same rows as step 3, and no way out to the host.

The threat model, including the leaks it accepts, is in SECURITY.md. It needs DuckDB 2.0.0, and it's submitted as a community extension (https://github.com/duckdb/community-extensions/pull/3570), so it's unsigned until that merges.

n6kd is a controller that does all of this in one binary, with policies from a file: `n6kd quack -f schema.sql --perms perms.yml`. https://github.com/n6k-io/n6kd

https://github.com/n6k-io/duckdb-virtual-catalog
