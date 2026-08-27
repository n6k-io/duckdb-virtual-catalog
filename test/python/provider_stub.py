"""A host-side provider implemented in Python, standing in for the real one.

It is deliberately literal about the contract: it answers with exactly the columns it was asked
for, in the order it was asked for them, and it applies exactly the filters it was handed. That
second part is load-bearing -- filter_json.hpp states that DuckDB does not re-apply filters pushed
into an Arrow scan, so if the provider ignores them the extension has silently returned wrong rows.
"""

import datetime
import decimal
import json

import pyarrow as pa

from conftest import BIGINT, BLOB, VARCHAR, VARCHAR_LIST, read_stream, schema_message, stream_bytes

PRIMARY_KEY_METADATA_KEY = b"vcat.primary_keys"


def _rebuild(value, tag):
    """A value that crossed as text carries a tag saying how to turn it back into a value."""
    if value is None or tag is None:
        return value
    if tag == "date":
        return datetime.date.fromisoformat(value)
    if tag == "timestamp":
        return datetime.datetime.fromisoformat(value)
    if tag == "int":
        return int(value)
    if tag == "decimal":
        return decimal.Decimal(value)
    if tag == "opaque":
        raise ValueError("provider refuses an opaque filter value")
    raise ValueError(f"unknown value tag {tag!r}")


def _matches(row, clause):
    col, op, value = clause[0], clause[1], clause[2]
    tag = clause[3] if len(clause) > 3 else None
    actual = row[col]
    if op == "is_null":
        return actual is None
    if op == "is_not_null":
        return actual is not None
    if op == "in":
        return actual is not None and actual in [_rebuild(v, tag) for v in value]
    expected = _rebuild(value, tag)
    if actual is None or expected is None:
        return False
    return {
        "=": lambda: actual == expected,
        "!=": lambda: actual != expected,
        ">": lambda: actual > expected,
        ">=": lambda: actual >= expected,
        "<": lambda: actual < expected,
        "<=": lambda: actual <= expected,
    }[op]()


class FakeProvider:
    def __init__(self, con, catalog="app", schema="main", writeable=True, editable=True):
        self.con = con
        self.catalog = catalog
        self.schema = schema
        self.tables: dict[str, pa.Table] = {}
        self.primary_keys: dict[str, list[str]] = {}
        self.calls: list[tuple] = []
        self._writeable = writeable
        self._editable = editable
        self._registered = False

    # --- host side ------------------------------------------------------------------------------

    def add_table(self, name, table, primary_key=()):
        self.tables[name] = table
        self.primary_keys[name] = list(primary_key)

    def rows(self, name):
        return self.tables[name].to_pylist()

    def register(self, prefix="p"):
        c = self.con
        c.create_function(f"{prefix}_list", self._list, [], VARCHAR)
        c.create_function(f"{prefix}_schema", self._schema, [VARCHAR], BLOB)
        c.create_function(f"{prefix}_scan", self._scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
        c.create_function(f"{prefix}_insert", self._insert, [VARCHAR, BLOB], BIGINT)
        c.create_function(f"{prefix}_update", self._update, [VARCHAR, BLOB, VARCHAR], BIGINT)
        c.create_function(f"{prefix}_delete", self._delete, [VARCHAR, BLOB], BIGINT)
        c.create_function(f"{prefix}_alter", self._alter, [VARCHAR, VARCHAR, VARCHAR], VARCHAR)
        c.execute(
            "SELECT vcat_register_provider(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                self.catalog,
                self.schema,
                "probe",
                f"{prefix}_list",
                f"{prefix}_schema",
                f"{prefix}_scan",
                f"{prefix}_insert" if self._writeable else "",
                f"{prefix}_update" if self._writeable else "",
                f"{prefix}_delete" if self._writeable else "",
                f"{prefix}_alter" if self._editable else "",
            ],
        )
        self._registered = True

    def invalidate(self):
        self.con.execute("SELECT vcat_invalidate_provider_tables(?, ?)", [self.catalog, self.schema])

    def unregister(self):
        self.con.execute("SELECT vcat_unregister_provider(?, ?)", [self.catalog, self.schema])
        self._registered = False

    # --- UDFs -----------------------------------------------------------------------------------

    def _list(self) -> str:
        self.calls.append(("list",))
        return "|".join(self.tables)

    def _schema(self, name: str) -> bytes:
        self.calls.append(("schema", name))
        table = self.tables.get(name)
        if table is None:
            return None
        schema = table.schema
        keys = self.primary_keys.get(name) or []
        if keys:
            schema = schema.with_metadata({PRIMARY_KEY_METADATA_KEY: ",".join(keys).encode()})
        return schema_message(schema)

    def _scan(self, name: str, columns, filters: str) -> bytes:
        self.calls.append(("scan", name, list(columns), filters))
        table = self.tables[name]
        rows = table.to_pylist()
        if filters:
            clauses = json.loads(filters)
            rows = [r for r in rows if all(_matches(r, c) for c in clauses)]
        columns = list(columns)
        # Deliberately naive: a Table built from an empty array list is zero rows no matter how many
        # matched. The extension never asks for zero columns for exactly this reason, and this stub
        # stays naive so that a regression there shows up as a wrong count.
        # Columns are answered positionally and may repeat: a key that is also projected is asked
        # for twice, because the row-id machinery reads the trailing key block by position.
        arrays = [pa.array([r[c] for r in rows], type=table.schema.field(c).type) for c in columns]
        names = [f"c{i}" for i in range(len(columns))]
        return stream_bytes(pa.table(arrays, names=names))

    def _insert(self, name: str, payload: bytes) -> int:
        incoming = read_stream(payload)
        self.calls.append(("insert", name, incoming.num_rows))
        existing = self.tables[name]
        self.tables[name] = pa.concat_tables([existing, incoming.rename_columns(existing.column_names)])
        return incoming.num_rows

    def _update(self, name: str, payload: bytes, changed_columns: str) -> int:
        incoming = read_stream(payload)
        self.calls.append(("update", name, changed_columns, incoming.num_rows))
        keys = self.primary_keys[name]
        changed = [c for c in changed_columns.split(",") if c]
        # Read positionally, never by name: the payload is the key columns (their OLD values, from
        # the scan) followed by the changed columns, and an UPDATE that touches the key itself puts
        # the same column name in both halves. Matching by name would silently look up the row by
        # the new key and find nothing.
        cols = [incoming.column(i).to_pylist() for i in range(incoming.num_columns)]
        rows = self.tables[name].to_pylist()
        for r in range(incoming.num_rows):
            key = {k: cols[i][r] for i, k in enumerate(keys)}
            for row in rows:
                if all(row[k] == v for k, v in key.items()):
                    for j, column in enumerate(changed):
                        row[column] = cols[len(keys) + j][r]
        self.tables[name] = pa.Table.from_pylist(rows, schema=self.tables[name].schema)
        return incoming.num_rows

    def _delete(self, name: str, payload: bytes) -> int:
        incoming = read_stream(payload).to_pylist()
        self.calls.append(("delete", name, len(incoming)))
        keys = self.primary_keys[name]
        doomed = [{k: r[k] for k in keys} for r in incoming]
        rows = [
            r for r in self.tables[name].to_pylist() if not any(all(r[k] == v for k, v in d.items()) for d in doomed)
        ]
        self.tables[name] = pa.Table.from_pylist(rows, schema=self.tables[name].schema)
        return len(incoming)

    def _alter(self, name: str, kind: str, details: str) -> str:
        self.calls.append(("alter", name, kind, details))
        detail = json.loads(details)
        table = self.tables[name]
        if kind == "add_column":
            table = table.append_column(detail["name"], pa.nulls(table.num_rows, type=pa.int32()))
        elif kind == "drop_column":
            table = table.drop_columns([detail["name"]])
        elif kind == "rename_column":
            table = table.rename_columns(
                [detail["new_name"] if c == detail["old_name"] else c for c in table.column_names]
            )
        self.tables[name] = table
        # No invalidate() here: this UDF runs inside the ALTER, on the same connection, and calling
        # back into it would deadlock. The extension evicts this table's cached entry itself once
        # the UDF returns.
        return "ok"
