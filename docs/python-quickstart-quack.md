# Python quick start: bridge over Quack

[Quack](https://duckdb.org/quack/) lets clients query a DuckDB database over the network, but it
can't limit which tables or rows they see.

The bridge can. You keep your data in a private source database, and Quack serves a bridge in front
of it. Clients only see the tables and rows you share.

## 1. Install

```sh
pip install duckdb==1.5.4
```

```python
import duckdb

# n6k's extension repository
REPO = "https://storage.googleapis.com/n6k-duckdb-release"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

    con.execute(f"INSTALL virtual_catalog_bridge FROM '{REPO}'")
    con.execute("LOAD virtual_catalog_bridge")
    return con
```

## 2. Source: data and policy

Say your source database has two tables:

`users`

| id | tenant_id | name  |
|----|-----------|-------|
| 1  | 42        | ada   |
| 2  | 42        | grace |
| 3  | 7         | linus |

`secrets`, which clients must never see.

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

Share `users` with tenant 42. They can read, insert and update their own rows, but not delete.
`secrets` isn't shared, so clients can't see it.

```python
# Create bridge b1; the token lets the target connect once
token = source.execute("""
    SELECT bridge_register_source('b1', 'memory')
""").fetchone()[0]

# The tenant this bridge is for
source.execute("""
    SELECT bridge_set_context('b1', 'tenant', 42)
""")

# Each action only touches tenant 42's rows
for verb in ["select", "insert", "update"]:
    source.execute(f"""
        SELECT bridge_set_policy('b1', 'main.users', '{verb}', 'tenant_id = $tenant')
    """)
```

## 3. Target: open the bridge

The target is the database Quack will serve. Open it as a bridge, then connect it to the source with
the token.

```python
target = duckdb.connect("virtual_catalog_bridge:", config={"allow_unsigned_extensions": "true"})
target.execute("SELECT virtual_catalog_bridge_init('b1', $1)", [token])
```

## 4. Serve the target over Quack

```python
target.execute("""
    INSTALL quack;  LOAD quack;
    INSTALL httpfs; LOAD httpfs;
""")

target.execute("""
    CALL quack_serve('quack:localhost:9494', token = 'super_secret')
""")
```

Load `httpfs` now: Quack needs it, and after lockdown it can't be loaded.

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

Lockdown stops clients from reading files, changing settings or loading extensions. Keep this
process running.

## 6. Query from a client

In another terminal, start `duckdb` and run:

```sql
INSTALL quack;
LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:localhost:9494' AS app;

-- Only tenant 42's rows
SELECT * FROM app.users;
-- (1, 42, 'ada'), (2, 42, 'grace')

-- Insert for tenant 42: allowed
INSERT INTO app.users VALUES (4, 42, 'barbara');
-- 1

-- Insert for another tenant: refused
INSERT INTO app.users VALUES (5, 7, 'mallory');
-- Invalid Input Error: Failed to append: virtual_catalog_bridge: row violates the check predicate on this table

-- Update tenant 42's row
FROM quack_query('quack:localhost:9494', 'UPDATE users SET name = ''Ada'' WHERE id = 1');
-- 1

-- Update another tenant's row: not visible, nothing changes
FROM quack_query('quack:localhost:9494', 'UPDATE users SET name = ''Linus'' WHERE id = 3');
-- 0

-- Delete: not granted
FROM quack_query('quack:localhost:9494', 'DELETE FROM users WHERE id = 2');
-- Invalid Input Error: crossing: 'users' does not have 'delete' permission

-- Not granted: doesn't exist
FROM app.secrets;
-- Catalog Error: Table with name secrets does not exist!

-- Locked down: no file access
FROM quack_query('quack:localhost:9494', 'SELECT * FROM read_csv(''/etc/passwd'')');
-- Invalid Input Error: File system LocalFileSystem has been disabled by configuration

-- Dropped by lockdown
FROM quack_query('quack:localhost:9494', 'CALL quack_stop(''quack:localhost:9494'')');
-- Invalid Input Error: Table Function with name "quack_stop" is not in the catalog, ...
```

Quack's `ATTACH` can't run `UPDATE` or `DELETE`, bridge or not, so send those through
`quack_query`.

Next: [python-quickstart-bridge.md](python-quickstart-bridge.md),
[virtual-catalog-bridge.md](virtual-catalog-bridge.md), [sql-api.md](sql-api.md).
