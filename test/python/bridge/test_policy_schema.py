"""The JSON Schema in docs/ and vcatbridge_configure_policy accept and refuse the same documents."""

import json
import pathlib

import duckdb
import jsonschema
import pytest

from conftest import unique_id

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCHEMA = json.loads((ROOT / "docs" / "policy-v1.schema.json").read_text())
CORPUS = ROOT / "test" / "policy_corpus"
VALID = sorted((CORPUS / "valid").glob("*.json"))
INVALID = sorted((CORPUS / "invalid").glob("*.json"))


@pytest.fixture
def pending(source):
    source.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, tenant INTEGER, name VARCHAR)")
    source.execute("CREATE TABLE orgs(id INTEGER PRIMARY KEY)")
    source.execute("CREATE TABLE members(id INTEGER PRIMARY KEY, org_id INTEGER REFERENCES orgs(id))")
    bridge_id = unique_id()
    source.execute("SELECT vcatbridge_register_source(?, 'memory')", [bridge_id])
    return source, bridge_id


def test_the_schema_is_a_valid_schema():
    jsonschema.Draft202012Validator.check_schema(SCHEMA)


@pytest.mark.parametrize("path", VALID, ids=[p.stem for p in VALID])
def test_valid_documents_pass_both(pending, path):
    source, bridge_id = pending
    jsonschema.validate(json.loads(path.read_text()), SCHEMA)
    assert source.execute("SELECT vcatbridge_configure_policy(?, ?)", [bridge_id, path.read_text()]).fetchone() == (
        "ok",
    )


@pytest.mark.parametrize("path", INVALID, ids=[p.stem for p in INVALID])
def test_invalid_documents_fail_both(pending, path):
    source, bridge_id = pending
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(json.loads(path.read_text()), SCHEMA)
    with pytest.raises(duckdb.IOException):
        source.execute("SELECT vcatbridge_configure_policy(?, ?)", [bridge_id, path.read_text()])


def test_documented_examples_are_valid():
    examples = []
    for doc in (ROOT / "docs").glob("*.md"):
        text = doc.read_text()
        for block in text.split("```json")[1:]:
            examples.append((doc.name, block.split("```")[0]))
    assert examples
    for name, block in examples:
        document = json.loads(block)
        if "policy_version" in document:
            jsonschema.validate(document, SCHEMA)


@pytest.mark.parametrize("path", VALID, ids=[p.stem for p in VALID])
def test_valid_documents_validate_clean(pending, path):
    source, _ = pending
    rows = source.execute("SELECT * FROM vcatbridge_validate_policy_config('memory', ?)", [path.read_text()]).fetchall()
    # A document that grants no table configures but cannot be attached.
    attach_only = (None, "error")
    assert [
        row for row in rows if row[1] != "unchecked" and not (row[:2] == attach_only and "has no policies" in row[2])
    ] == []


@pytest.mark.parametrize("path", INVALID, ids=[p.stem for p in INVALID])
def test_invalid_documents_report_an_error(pending, path):
    source, _ = pending
    rows = source.execute("SELECT * FROM vcatbridge_validate_policy_config('memory', ?)", [path.read_text()]).fetchall()
    assert any(row[1] in ("error", "data_dependent") for row in rows)
