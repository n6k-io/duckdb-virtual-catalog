# Python quick start: bridge

## 1. Install

```sh
pip install duckdb==1.5.6
```

```python
import duckdb

REPO = "https://storage.googleapis.com/n6k-duckdb-release"

def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"INSTALL virtual_catalog_bridge FROM '{REPO}'")
    con.execute("LOAD virtual_catalog_bridge")
    return con
```

## 2. Source: data and policy

```python
source = connect()
source.execute("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR);
    INSERT INTO users VALUES (1, 42, 'ada'), (2, 42, 'grace'), (3, 7, 'linus');
    CREATE TABLE secrets (k VARCHAR, v VARCHAR);
""")

token = source.execute("SELECT bridge_register_source('b1', 'memory')").fetchone()[0]
source.execute("SELECT bridge_set_context('b1', 'tenant', 42)")
source.execute("SELECT bridge_set_policy('b1', 'main.users', 'select', 'tenant_id = $tenant')")
source.execute("SELECT bridge_set_policy('b1', 'main.users', 'update', 'tenant_id = $tenant')")
```

## 3. Target: attach

```python
target = connect()
target.execute(f"ATTACH '' AS app (TYPE virtual_catalog_bridge, ID 'b1', TOKEN '{token}')")
```

The token expires after 30 seconds if no `ATTACH` redeems it.

## 4. Lock down the target

Cap memory and threads, turn off file and network access, and lock the configuration.

```python
target.execute("SELECT virtual_catalog_lockdown('1GB', 2)")
```

## 5. Run user SQL on the target

Any SQL can run here. It only reaches what the policy allows.

```python
target.execute("SELECT * FROM app.main.users").fetchall()
# [(1, 42, 'ada'), (2, 42, 'grace')]

target.execute("UPDATE app.main.users SET name = 'Ada' WHERE id = 1")

target.execute("SELECT * FROM app.main.secrets")
# CatalogException: Table with name secrets does not exist!

target.execute("DELETE FROM app.main.users")
# PermissionException: crossing: 'users' does not have 'delete' permission

target.execute("SELECT * FROM read_csv('/etc/passwd')")
# PermissionException: File system LocalFileSystem has been disabled by configuration

target.execute("SET threads = 8")
# InvalidInputException: Cannot change configuration option "threads" - the configuration has been locked
```

## 6. Switch tenant

```python
source.execute("SELECT bridge_set_context('b1', 'tenant', 7)")

target.execute("SELECT * FROM app.main.users").fetchall()
# [(3, 7, 'linus')]
```

Next: [virtual-catalog-bridge.md](virtual-catalog-bridge.md), [sql-api.md](sql-api.md).
