# Python quick start: provider

## 1. Install

```sh
pip install duckdb==1.5.6 pyarrow numpy
```

```python
import json
import operator

import duckdb
import pyarrow as pa

REPO = "https://storage.googleapis.com/n6k-duckdb-release"

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"INSTALL virtual_catalog_provider FROM '{REPO}'")
con.execute("LOAD virtual_catalog_provider")
```

## 2. Your data

Any Python data. Here, a dict of Arrow tables.

```python
TABLES = {
    "main.users": pa.table({"id": [1, 2, 3], "name": ["ada", "grace", "linus"]}),
}
```

## 3. Three functions: list, schema, scan

```python
OPS = {"=": operator.eq, "!=": operator.ne, "<": operator.lt,
       "<=": operator.le, ">": operator.gt, ">=": operator.ge}

def list_tables():
    return "|".join(TABLES)

def schema(name):
    return TABLES[name].schema.serialize().to_pybytes()

def scan(name, columns, filters):
    rows = TABLES[name].to_pylist()
    # DuckDB does not re-check pushed filters: the scan must apply them.
    for col, op, value, *_ in json.loads(filters or "[]"):
        rows = [r for r in rows if r[col] is not None and OPS[op](r[col], value)]
    out = pa.table({f"c{i}": pa.array([r[c] for r in rows], TABLES[name].schema.field(c).type)
                    for i, c in enumerate(columns)})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, out.schema) as w:
        w.write_table(out)
    return sink.getvalue().to_pybytes()

VARCHAR, BLOB = duckdb.sqltype("VARCHAR"), duckdb.sqltype("BLOB")
con.create_function("p_list", list_tables, [], VARCHAR)
con.create_function("p_schema", schema, [VARCHAR], BLOB)
con.create_function("p_scan", scan, [VARCHAR, duckdb.sqltype("VARCHAR[]"), VARCHAR], BLOB)
```

## 4. Attach

```python
con.execute("ATTACH '' AS app (TYPE virtual_catalog_provider, list 'p_list', schema 'p_schema', scan 'p_scan')")
```

## 5. Query

```python
con.execute("SELECT * FROM app.main.users").fetchall()
# [(1, 'ada'), (2, 'grace'), (3, 'linus')]

con.execute("SELECT name FROM app.main.users WHERE id > 1").fetchall()
# [('grace',), ('linus',)]

con.execute("INSERT INTO app.main.users VALUES (4, 'barbara')")
# PermissionException: Provider table 'users' does not support INSERT
```

Add `insert`, `update` and `delete` functions to make it writable.

Next: [virtual-catalog-provider.md](virtual-catalog-provider.md), [sql-api.md](sql-api.md).
