"""The frame codec: pack_frame / unpack_frame / validate_header."""

import pytest

from n6k_protocol.engine import pack_frame, unpack_frame, validate_header
from n6k_protocol.protocol import FT_REQ, OP_INSERT, OP_QUERY


def test_round_trip_without_body():
    header = {"t": FT_REQ, "id": 1, "op": OP_QUERY, "sql": "select 1"}
    decoded, body = unpack_frame(pack_frame(header))
    assert decoded == header
    assert len(body) == 0


def test_round_trip_with_arrow_body():
    arrow = b"ARROW\x00BYTES"
    header = {"t": FT_REQ, "id": 2, "op": OP_INSERT, "schema": "s", "table": "t"}
    decoded, body = unpack_frame(pack_frame(header, arrow))
    assert decoded == header
    assert bytes(body) == arrow


def test_unknown_frame_type_is_rejected():
    with pytest.raises(ValueError, match="unknown frame type"):
        validate_header({"t": 250})


def test_missing_required_field_is_rejected():
    with pytest.raises(ValueError):
        pack_frame({"t": FT_REQ, "op": OP_QUERY, "sql": "select 1"})


def test_short_frame_is_rejected():
    with pytest.raises(ValueError, match="short frame"):
        unpack_frame(b"")
