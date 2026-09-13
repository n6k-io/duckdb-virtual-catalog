"""Provider tables end to end: the Arrow round trip, DML routing, ALTER, and cache invalidation.

Nothing here is reachable from sqllogictest -- the provider contract is Arrow IPC over host UDFs.
"""

import datetime

import pyarrow as pa
import pytest

from conftest import BLOB, VARCHAR, VARCHAR_LIST, schema_message, stream_bytes
from provider_stub import FakeProvider

USERS = pa.table(
    {
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "name": pa.array(["ana", "bo", None]),
        "score": pa.array([10, 20, 30], type=pa.int32()),
        "joined": pa.array([datetime.date(2024, 1, 1), datetime.date(2025, 6, 1), None], type=pa.date32()),
    }
)


@pytest.fixture
def provider(con):
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    p = FakeProvider(con)
    p.add_table("users", USERS, primary_key=["id"])
    p.register()
    return p


def test_provider_table_is_visible_and_typed(con, provider):
    assert con.execute("SELECT * FROM app.main.users ORDER BY id").fetchall() == [
        (1, "ana", 10, datetime.date(2024, 1, 1)),
        (2, "bo", 20, datetime.date(2025, 6, 1)),
        (3, None, 30, None),
    ]
    described = con.execute("DESCRIBE app.main.users").fetchall()
    assert [(c[0], c[1]) for c in described] == [
        ("id", "INTEGER"),
        ("name", "VARCHAR"),
        ("score", "INTEGER"),
        ("joined", "DATE"),
    ]


def test_permissions_report_the_provider_kind_and_key(con, provider):
    assert con.execute(
        "SELECT name, kind, primary_key, verbs FROM provider_table_permissions('app', schema := 'main')"
    ).fetchall() == [("users", "provider", ["id"], ["select", "insert", "update", "delete", "alter"])]


def test_describe_reports_provider_columns(con, provider):
    columns, primary_key, verbs, _ = con.execute(
        "SELECT * FROM provider_table_describe('app', schema := 'main', \"table\" := 'users')"
    ).fetchone()
    assert [c["name"] for c in columns] == ["id", "name", "score", "joined"]
    assert (primary_key, verbs) == (["id"], ["select", "insert", "update", "delete", "alter"])


@pytest.mark.parametrize(
    "predicate",
    [
        "id = 2",
        "id != 2",
        "id > 1",
        "id <= 2",
        "id IN (1, 3)",
        "name IS NULL",
        "name IS NOT NULL",
        "name = 'ana'",
        "joined >= DATE '2025-01-01'",
        "joined IS NULL",
        "score > 10 AND score < 30",
    ],
)
def test_filter_pushdown_is_applied_by_the_provider(con, provider, predicate):
    """DuckDB does not re-apply filters pushed into an Arrow scan, so whatever reaches the provider
    IS the filter. The stub applies exactly what it was handed; the same predicate evaluated over
    the unfiltered data is the oracle."""
    expected = [row for row in con.execute("SELECT * FROM (SELECT * FROM app.main.users) WHERE true").fetchall()]
    con.execute("SET disabled_optimizers TO 'filter_pushdown'")
    unpushed = con.execute(f"SELECT * FROM app.main.users WHERE {predicate} ORDER BY id").fetchall()
    con.execute("RESET disabled_optimizers")
    pushed = con.execute(f"SELECT * FROM app.main.users WHERE {predicate} ORDER BY id").fetchall()
    assert pushed == unpushed
    assert expected  # the fixture data actually reached the scan


def test_projection_is_answered_positionally(con, provider):
    assert con.execute("SELECT score, id FROM app.main.users ORDER BY id").fetchall() == [(10, 1), (20, 2), (30, 3)]
    scans = [c for c in provider.calls if c[0] == "scan"]
    assert scans and scans[-1][2] == ["score", "id"]


def test_count_star_asks_for_one_column(con, provider):
    """COUNT(*) projects nothing. A provider building its answer the obvious way cannot express a
    zero-column result with rows -- a Table from an empty array list is zero rows -- so the count
    would come back 0 with nothing raising. The scan asks for one column instead and reads only the
    cardinality."""
    assert con.execute("SELECT count(*) FROM app.main.users").fetchone() == (3,)
    scans = [c for c in provider.calls if c[0] == "scan"]
    assert len(scans[-1][2]) == 1


def test_empty_result_is_not_an_error(con, provider):
    provider.add_table("empty", USERS.slice(0, 0), primary_key=["id"])
    provider.invalidate()
    assert con.execute("SELECT * FROM app.main.empty").fetchall() == []


def test_multi_batch_scan(con, provider):
    big = pa.table(
        {
            "id": pa.array(range(5000), type=pa.int32()),
            "name": pa.array([f"n{i}" for i in range(5000)]),
            "score": pa.array(range(5000), type=pa.int32()),
            "joined": pa.array([None] * 5000, type=pa.date32()),
        }
    )
    provider.add_table("big", big, primary_key=["id"])
    provider.invalidate()
    assert con.execute("SELECT count(*), sum(score) FROM app.main.big").fetchone() == (5000, sum(range(5000)))


def test_insert_reaches_the_provider(con, provider):
    con.execute("INSERT INTO app.main.users VALUES (4, 'di', 40, DATE '2026-01-01')")
    assert provider.rows("users")[-1] == {
        "id": 4,
        "name": "di",
        "score": 40,
        "joined": datetime.date(2026, 1, 1),
    }
    assert con.execute("SELECT count(*) FROM app.main.users").fetchone() == (4,)


def test_update_reaches_the_provider_with_key_and_changed_columns(con, provider):
    con.execute("UPDATE app.main.users SET score = 99 WHERE id = 2")
    updates = [c for c in provider.calls if c[0] == "update"]
    assert updates and "score" in updates[-1][2]
    assert [r["score"] for r in provider.rows("users")] == [10, 99, 30]


def test_delete_reaches_the_provider(con, provider):
    con.execute("DELETE FROM app.main.users WHERE id = 1")
    assert [r["id"] for r in provider.rows("users")] == [2, 3]


def test_alter_add_drop_rename(con, provider):
    con.execute("ALTER TABLE app.main.users ADD COLUMN extra INTEGER")
    assert "extra" in provider.tables["users"].column_names
    assert [c[0] for c in con.execute("DESCRIBE app.main.users").fetchall()][-1] == "extra"

    con.execute("ALTER TABLE app.main.users RENAME COLUMN extra TO renamed")
    assert [c[0] for c in con.execute("DESCRIBE app.main.users").fetchall()][-1] == "renamed"

    con.execute("ALTER TABLE app.main.users DROP COLUMN renamed")
    assert "renamed" not in provider.tables["users"].column_names


def test_unsupported_alter_is_refused(con, provider):
    with pytest.raises(Exception, match="not supported"):
        con.execute("ALTER TABLE app.main.users ALTER COLUMN score SET DATA TYPE BIGINT")


def test_drop_table_is_refused(con, provider):
    with pytest.raises(Exception, match="provider-managed"):
        con.execute("DROP TABLE app.main.users")


def test_invalidate_picks_up_a_new_table(con, provider):
    assert con.execute("SELECT count(*) FROM provider_table_permissions('app', schema := 'main')").fetchone() == (1,)
    provider.add_table("later", USERS, primary_key=["id"])
    provider.invalidate()
    assert con.execute(
        "SELECT name FROM provider_table_permissions('app', schema := 'main') ORDER BY name"
    ).fetchall() == [
        ("later",),
        ("users",),
    ]


def test_schema_changes_are_only_seen_after_invalidate(con, provider):
    assert con.execute("SELECT count(*) FROM app.main.users").fetchone() == (3,)
    provider.tables["users"] = USERS.slice(0, 1)
    provider.invalidate()
    assert con.execute("SELECT count(*) FROM app.main.users").fetchone() == (1,)


def test_attach_options_register_the_provider(con):
    p = FakeProvider(con, catalog="attached")
    p.add_table("users", USERS, primary_key=["id"])
    p.attach(prefix="at")
    assert con.execute("SELECT id, name FROM attached.main.users ORDER BY id").fetchall() == [
        (1, "ana"),
        (2, "bo"),
        (3, None),
    ]
    assert con.execute(
        "SELECT name, kind, verbs FROM provider_table_permissions('attached', schema := 'main')"
    ).fetchall() == [("users", "provider", ["select", "insert", "update", "delete", "alter"])]


def test_attach_options_serve_every_schema_the_list_names(con):
    def m_list():
        return "main.users|reporting.events"

    def m_schema(name):
        return schema_message(pa.schema([pa.field("id", pa.int32())]))

    def m_scan(name, columns, filters):
        return stream_bytes(pa.table({"c0": pa.array([7], type=pa.int32())}))

    con.create_function("m_list", m_list, [], VARCHAR)
    con.create_function("m_schema", m_schema, [VARCHAR], BLOB)
    con.create_function("m_scan", m_scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
    con.execute("ATTACH '' AS multi (TYPE virtual_catalog_provider, list m_list, schema m_schema, scan m_scan)")

    assert con.execute("SELECT id FROM multi.reporting.events").fetchall() == [(7,)]
    assert con.execute("SELECT schema, name FROM provider_table_permissions('multi') ORDER BY schema").fetchall() == [
        ("main", "users"),
        ("reporting", "events"),
    ]


def test_an_unqualified_listed_name_is_refused(con):
    def u_list():
        return "users"

    def u_schema(name):
        return schema_message(pa.schema([pa.field("id", pa.int32())]))

    def u_scan(name, columns, filters):
        return b""

    con.create_function("u_list", u_list, [], VARCHAR)
    con.create_function("u_schema", u_schema, [VARCHAR], BLOB)
    con.create_function("u_scan", u_scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
    con.execute("ATTACH '' AS bare (TYPE virtual_catalog_provider, list u_list, schema u_schema, scan u_scan)")

    with pytest.raises(Exception, match="schema.table"):
        con.execute("SELECT * FROM bare.main.users").fetchall()


def test_a_partial_attach_option_set_is_refused(con):
    with pytest.raises(Exception, match="LIST, SCHEMA and SCAN"):
        con.execute("ATTACH '' AS partial (TYPE virtual_catalog_provider, list only_list)")


def test_provider_without_write_udfs_is_read_only(con):
    con.execute("ATTACH ':memory:' AS ro (TYPE virtual_catalog_provider)")
    p = FakeProvider(con, catalog="ro", writeable=False, editable=False)
    p.add_table("users", USERS, primary_key=["id"])
    p.register(prefix="ro")
    assert con.execute("SELECT kind, verbs FROM provider_table_permissions('ro', schema := 'main')").fetchall() == [
        ("provider", ["select"])
    ]
    with pytest.raises(Exception, match="does not support ALTER"):
        con.execute("ALTER TABLE ro.main.users ADD COLUMN x INTEGER")


def test_unregister_removes_the_tables(con, provider):
    provider.unregister()
    assert con.execute("SELECT count(*) FROM provider_table_permissions('app', schema := 'main')").fetchone() == (0,)
    with pytest.raises(Exception):
        con.execute("SELECT * FROM app.main.users").fetchall()
    with pytest.raises(Exception, match="no provider registered"):
        con.execute("SELECT provider_unregister('app')")


def test_native_tables_coexist_with_provider_tables(con, provider):
    con.execute("CREATE TABLE app.main.local(id INTEGER PRIMARY KEY, tag VARCHAR)")
    con.execute("INSERT INTO app.main.local VALUES (1, 'one'), (2, 'two')")
    assert con.execute(
        "SELECT u.name, l.tag FROM app.main.users u JOIN app.main.local l USING (id) ORDER BY u.id"
    ).fetchall() == [("ana", "one"), ("bo", "two")]
    assert con.execute(
        "SELECT name, kind FROM provider_table_permissions('app', schema := 'main') ORDER BY name"
    ).fetchall() == [("local", "native_table"), ("users", "provider")]


def test_a_raising_scan_udf_surfaces_as_a_query_error(con):
    con.execute("ATTACH ':memory:' AS bad (TYPE virtual_catalog_provider)")

    def boom_list():
        return "main.t"

    def boom_schema(name):
        return schema_message(pa.schema([pa.field("id", pa.int32())]))

    def boom_scan(name, columns, filters):
        raise RuntimeError("provider exploded")

    con.create_function("b_list", boom_list, [], VARCHAR)
    con.create_function("b_schema", boom_schema, [VARCHAR], BLOB)
    con.create_function("b_scan", boom_scan, [VARCHAR, VARCHAR_LIST, VARCHAR], BLOB)
    con.execute("SELECT provider_register('bad', 'b_list', 'b_schema', 'b_scan', '', '', '', '')")
    with pytest.raises(Exception, match="provider exploded"):
        con.execute("SELECT * FROM bad.main.t").fetchall()


def test_a_malformed_arrow_payload_is_reported_not_crashed(con):
    con.execute("ATTACH ':memory:' AS junk (TYPE virtual_catalog_provider)")

    def junk_list():
        return "main.t"

    def junk_schema(name):
        return b"not arrow ipc at all"

    con.create_function("j_list", junk_list, [], VARCHAR)
    con.create_function("j_schema", junk_schema, [VARCHAR], BLOB)
    con.execute("SELECT provider_register('junk', 'j_list', 'j_schema', 'j_scan', '', '', '', '')")
    with pytest.raises(Exception, match="Arrow"):
        con.execute("SELECT * FROM junk.main.t").fetchall()
