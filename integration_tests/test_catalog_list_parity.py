"""OP_CATALOG_LIST: every server, one implementation.

Each target answers this op by calling `ListCatalogSchemas`
(src/common/n6k_catalog_list.cpp) — the Python side through the `n6k_catalog_list`
table function — rather than each carrying its own `information_schema.schemata`
query. They previously did carry their own, and had already drifted: the C++ copy
ended with `ORDER BY schema_name` and the Python copy did not, so the two returned
the same schemas in different orders.

Every target seeds the same catalog from the same `fixtures.sql`, which is what
makes them directly comparable.

Run:
    uv run pytest integration_tests/test_catalog_list_parity.py -v
"""

import pytest

from _targets import TARGETS, WireClient, connect
from n6k_protocol.protocol import FT_RESP_CHUNK, FT_RESP_END, OP_CATALOG_LIST


async def _catalog_list(client: WireClient) -> list[str]:
    """One OP_CATALOG_LIST round trip, returning the schemas."""
    frames = await client.request(OP_CATALOG_LIST)
    kinds = [h["t"] for h, _ in frames]
    assert kinds == [FT_RESP_CHUNK, FT_RESP_END], kinds
    return list(frames[0][0]["schemas"])


@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.asyncio
async def test_catalog_list_is_sorted_and_excludes_system_schemas(server, target) -> None:
    async with connect(target) as (client, _):
        schemas = await _catalog_list(client)

    assert "main" in schemas
    assert schemas == sorted(schemas), f"{target} returned schemas out of order: {schemas}"
    assert "information_schema" not in schemas
    assert "pg_catalog" not in schemas
    # A schema must not be reported twice.
    assert len(schemas) == len(set(schemas))


@pytest.mark.asyncio
async def test_all_servers_return_identical_schema_lists(server) -> None:
    """Same catalog, same op — every server must agree exactly, order included.
    This is the assertion that would have caught the ORDER BY drift."""
    results = {}
    for target in TARGETS:
        async with connect(target) as (client, _):
            results[target.id] = await _catalog_list(client)

    reference_id, reference = next(iter(results.items()))
    for target_id, schemas in results.items():
        assert schemas == reference, f"{target_id} disagrees with {reference_id}: {schemas} != {reference}"
