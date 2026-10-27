# Python quick start: bridge over Quack

[Quack](https://duckdb.org/quack/) serves a DuckDB database over the network. Its permission hook
sees only each query's SQL text — no per-table or per-row grants.

Serve Quack from a bridge target instead: clients reach only the tables and rows the policy
grants, and the source database is never served.

## 1. Install

```sh
pip install duckdb==1.5.4
```

```python
import duckdb

# n6k's extension repository
REPO = "https://storage.googleapis.com/n6k-duckdb-release"


def connect():
    # A new, separate DuckDB database
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

    con.execute(f"INSTALL virtual_catalog_bridge FROM '{REPO}'")
    con.execute("LOAD virtual_catalog_bridge")
    return con
```

## 2. Source: data and policy

Imagine a source database with two tables:

`users`

| id | tenant_id | name  |
|----|-----------|-------|
| 1  | 42        | ada   |
| 2  | 42        | grace |
| 3  | 7         | linus |

`secrets` — must never be served.

<details>
<summary>Create them to follow along</summary>

```python
source = connect()
source.execute("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
    INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
    CREATE TABLE secrets (k VARCHAR, v VARCHAR);
""")
```

</details>

Share `users` with tenant 42: read, insert and update its own rows, no delete. `secrets` is not
granted.

```python
# Register the bridge; returns a one-time token for the target's ATTACH
token = source.execute("""
    SELECT bridge_register_source('b1', 'memory')
""").fetchone()[0]

# $tenant, used by the row filters below
source.execute("""
    SELECT bridge_set_context('b1', 'tenant', 42)
""")

# Each verb only reaches rows where tenant_id = $tenant
for verb in ["select", "insert", "update"]:
    source.execute(f"""
        SELECT bridge_set_policy('b1', 'main.users', '{verb}', 'tenant_id = $tenant')
    """)
```

## 3. Target: attach

```python
target = connect()

# Redeem the token: granted tables appear under app
target.execute(f"""
    ATTACH '' AS app (
        TYPE virtual_catalog_bridge,
        ID 'b1',
        TOKEN '{token}'
    )
""")
```

## 4. Serve the target over Quack

```python
# httpfs must be loaded before lockdown (see below)
target.execute("""
    INSTALL quack;  LOAD quack;
    INSTALL httpfs; LOAD httpfs;
""")

target.execute("""
    CALL quack_serve('quack:localhost:9494', token = 'super_secret')
""")
```

The Quack server loads `httpfs` on demand. After lockdown it can't, and every request would fail
with HTTP 500.

## 5. Lock down the target

```python
target.execute("""
    SELECT virtual_catalog_lockdown(
        '1GB',                         -- memory limit
        2,                             -- threads
        ['quack_serve', 'quack_stop']  -- clients can't start or stop servers
    )
""")
```

Keep this process running.

## 6. Query from a client

In another process, plain SQL. `quack_query` runs its SQL on the server.

```sql
INSTALL quack;
LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret');

-- Only tenant 42's rows
FROM quack_query('quack:localhost:9494', 'SELECT * FROM app.main.users');
-- (1, 42, 'ada'), (2, 42, 'grace')

-- Insert for tenant 42: allowed
FROM quack_query('quack:localhost:9494', 'INSERT INTO app.main.users VALUES (4, 42, ''barbara'')');
-- 1

-- Insert for another tenant: refused
FROM quack_query('quack:localhost:9494', 'INSERT INTO app.main.users VALUES (5, 7, ''mallory'')');
-- Invalid Input Error: virtual_catalog_bridge: row violates the check predicate on this table

-- Update tenant 42's row
FROM quack_query('quack:localhost:9494', 'UPDATE app.main.users SET name = ''Ada'' WHERE id = 1');
-- 1

-- Update another tenant's row: not visible, nothing changes
FROM quack_query('quack:localhost:9494', 'UPDATE app.main.users SET name = ''Linus'' WHERE id = 3');
-- 0

-- Delete: not granted
FROM quack_query('quack:localhost:9494', 'DELETE FROM app.main.users WHERE id = 2');
-- Invalid Input Error: crossing: 'users' does not have 'delete' permission

-- Not granted: doesn't exist
FROM quack_query('quack:localhost:9494', 'SELECT * FROM app.main.secrets');
-- Invalid Input Error: Table with name secrets does not exist!

-- Locked down: no file access
FROM quack_query('quack:localhost:9494', 'SELECT * FROM read_csv(''/etc/passwd'')');
-- Invalid Input Error: File system LocalFileSystem has been disabled by configuration

-- Dropped by lockdown
FROM quack_query('quack:localhost:9494', 'CALL quack_stop(''quack:localhost:9494'')');
-- Invalid Input Error: Table Function with name "quack_stop" is not in the catalog, ...
```

Use `quack_query`, not `ATTACH 'quack:...'`: `ATTACH` only exposes the server's default catalog,
not `app` ([duckdb-quack#185](https://github.com/duckdb/duckdb-quack/issues/185)).

Next: [python-quickstart-bridge.md](python-quickstart-bridge.md),
[virtual-catalog-bridge.md](virtual-catalog-bridge.md), [sql-api.md](sql-api.md).
