# Per-row policies for Quack

Quack lets clients query a DuckDB database over the network, but it can't limit which tables or rows they see.

I wrote vcatbridge, a DuckDB extension that can. Here's a walkthrough with two DuckDB instances in one process: DB1 has the data, and DB2 is what you hand to someone else. A controller opens both and keeps DB1 to itself.

**1. Create some data on DB1**

```sql
-- DB1
LOAD vcatbridge;

CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
CREATE TABLE secrets (k VARCHAR, v VARCHAR);
```

Two tenants in `users`, and a `secrets` table nobody else should see.

**2. Set up the bridge on DB1**

Register a bridge, say which tenant it's for, and grant `users` to that tenant. `secrets` gets no policy, and there's no `delete` on anything.

```sql
-- DB1
SELECT vcatbridge_register_source('b1', 'memory');   -- returns a token
SELECT vcatbridge_set_context('b1', 'tenant', 42);
SELECT vcatbridge_set_policy('b1', 'main.users', 'select', 'tenant_id = $tenant');
SELECT vcatbridge_set_policy('b1', 'main.users', 'insert', 'tenant_id = $tenant');
SELECT vcatbridge_set_policy('b1', 'main.users', 'update', 'tenant_id = $tenant');
```

Only DB1 can change these. The policy and context functions refuse any instance other than the one that registered the bridge.

**3. Connect DB2 and query**

The controller opens DB2 as `vcatbridge:` and passes it the token:

```sql
-- DB2
SELECT vcatbridge_init('b1', '<token>');
```

DB2 can now query DB1's tables, through the policy:

```sql
-- DB2
SELECT * FROM users;
-- (1, 42, 'ada'), (2, 42, 'grace')

INSERT INTO users VALUES (5, 7, 'mallory');
-- Invalid Input Error: vcatbridge: row violates the check predicate on this table

DELETE FROM users WHERE id = 2;
-- Permission Error: crossing: 'users' does not have 'delete' permission

FROM secrets;
-- Catalog Error: Table with name secrets does not exist!
```

`secrets` isn't hidden behind a permission error. It isn't in DB2's catalog, so there's nothing to name.

The query crosses to DB1 as a plan, never as SQL text. Filters, joins and aggregates go with it, and the row filter is applied on DB1, under everything else. For a join of `users` with an `orders` table:

```
CROSSING_READ                      ← everything below runs on DB1
└─ PROJECTION   name, sum(amount)
   └─ AGGREGATE  group by name
      └─ JOIN    id = user_id
         ├─ users    (row filter: tenant_id = $tenant)
         └─ orders
```

A function crosses to DB1 only if it's on the bridge's whitelist. Anything else, such as `current_setting`, runs on DB2.

**4. Serve DB2 over Quack, and lock it down**

```sql
-- DB2
SET allow_persistent_secrets = false;
INSTALL quack;  LOAD quack;
INSTALL httpfs; LOAD httpfs;

CALL quack_serve('quack:localhost:9494', token = 'super_secret');
```

That already serves the policy. To also keep clients off the host, lock DB2 down:

```sql
-- DB2
SELECT vcatbridge_lockdown('1GB', 2, ['quack_serve']);
```

Lockdown takes files, network, extensions and settings away from DB2, caps its memory and threads, and drops `quack_serve` so clients can't start servers.

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
