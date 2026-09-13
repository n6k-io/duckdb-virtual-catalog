"""Single source of truth for protocol constants.

Edit this file to change any cross-language protocol value, then run:

    make protocol-gen

…to regenerate every mirror: the TypeScript and C++ headers and this package's
`schema.json`. The full list is `PROTOCOL_GENERATED` in the Makefile. Note that
Python's runtime header validation reads the generated `schema.json`, so editing
`_FRAME_HEADERS` without regenerating changes nothing at runtime.

`make protocol-check` verifies the generated files are up to date, but nothing
runs it automatically — there is no pre-commit config and no CI job invoking it.

This module is imported directly by integration tests; TypeScript, C++ and Go
consume the generated mirror files.
"""

from __future__ import annotations

from typing import Any, Final, Union

# Bump on incompatible protocol changes (frame layout, op semantics, etc.).
# Checked client-against-server on connect (ws-worker.ts, src/n6k_client/ws_client.cpp);
# there is no cross-version interop.
N6K_PROTOCOL_VERSION = 6

# ── Frame types ──────────────────────────────────────────────────────────────
# `Final` so the values are usable as `Literal[...]` discriminators in the
# generated `protocol_types.py` (a bare `FT_REQ = 1` would type as `int`).
FT_REQ: Final = 1
FT_RESP_SCHEMA: Final = 2
FT_RESP_CHUNK: Final = 3
FT_RESP_END: Final = 4
FT_ERR: Final = 5
FT_CANCEL: Final = 6
FT_CREDIT: Final = 7
FT_PUSH: Final = 8
FT_PING: Final = 9
FT_PONG: Final = 10
FT_HELLO: Final = 11  # C→S auth handshake: payload JSON {"token": ...}, sent before HELLO_ACK
FT_HELLO_ACK: Final = 12
FT_HELLO_ERR: Final = 13  # S→C connection-scoped failure in lieu of HELLO_ACK (auth / session
#                    factory): req_id=0, JSON {exception_type, exception_message}. The
#                    connect waiter surfaces it instead of a bare WS close.
FT_READY: Final = 14  # S→C session/catalog usable: empty body, req_id=0. Sent after a
#                HELLO_ACK that carried {"session_pending": true} once the deferred
#                session build completes; build failure → HELLO_ERR instead. Fast
#                (non-deferred) sessions send a ready HELLO_ACK and never emit READY.

_FRAME_TYPES = [
    ("REQ", FT_REQ),
    ("RESP_SCHEMA", FT_RESP_SCHEMA),
    ("RESP_CHUNK", FT_RESP_CHUNK),
    ("RESP_END", FT_RESP_END),
    ("ERR", FT_ERR),
    ("CANCEL", FT_CANCEL),
    ("CREDIT", FT_CREDIT),
    ("PUSH", FT_PUSH),
    ("PING", FT_PING),
    ("PONG", FT_PONG),
    ("HELLO", FT_HELLO),
    ("HELLO_ACK", FT_HELLO_ACK),
    ("HELLO_ERR", FT_HELLO_ERR),
    ("READY", FT_READY),
]

# ── Frame flags ─────────────────────────────────────────────────────────────
# Legacy: no longer carried on the wire. Headers are msgpack maps with no `flags`
# key, and the C++ client synthesizes these locally from the frame type
# (src/n6k_client/ws_client.cpp). Kept because the generated mirrors still expose them.
FLAG_END_OF_STREAM = 0x01
FLAG_COMPRESSED = 0x02  # reserved; not implemented
FLAG_IS_ARROW_IPC = 0x04

_FLAGS = [
    ("END_OF_STREAM", FLAG_END_OF_STREAM),
    ("COMPRESSED", FLAG_COMPRESSED),
    ("IS_ARROW_IPC", FLAG_IS_ARROW_IPC),
]

# ── REQ op codes ─────────────────────────────────────────────────────────────
# `Final` for the same reason as the frame types: usable as `Literal[...]`.
OP_CATALOG_LIST: Final = 1
OP_TABLES_LIST: Final = 2
OP_TABLE_SCHEMA: Final = 3
OP_SCAN: Final = 4
OP_INSERT: Final = 5
OP_EXEC: Final = 6
OP_QUERY: Final = 7
OP_RPC_SCALAR: Final = 8
OP_RPC_TABLE: Final = 9
OP_CREATE_TABLE: Final = 10
OP_ALTER_TABLE: Final = 11
OP_CATALOG_INVALIDATED: Final = 12
OP_AGGREGATE: Final = 13

_OPS = [
    ("CATALOG_LIST", OP_CATALOG_LIST),
    ("TABLES_LIST", OP_TABLES_LIST),
    ("TABLE_SCHEMA", OP_TABLE_SCHEMA),
    ("SCAN", OP_SCAN),
    ("INSERT", OP_INSERT),
    ("EXEC", OP_EXEC),
    ("QUERY", OP_QUERY),
    ("RPC_SCALAR", OP_RPC_SCALAR),
    ("RPC_TABLE", OP_RPC_TABLE),
    ("CREATE_TABLE", OP_CREATE_TABLE),
    ("ALTER_TABLE", OP_ALTER_TABLE),
    ("CATALOG_INVALIDATED", OP_CATALOG_INVALIDATED),
    ("AGGREGATE", OP_AGGREGATE),
]


def op_name(op: int) -> str:
    for name, code in _OPS:
        if code == op:
            return name
    return f"OP_{op}"


def frame_type_name(ft: int) -> str:
    for name, code in _FRAME_TYPES:
        if code == ft:
            return name
    return f"FT_{ft}"


# ── Frame header schema (SSOT for the msgpack header map of every frame) ──────
#
# Each frame is one msgpack map (the header) optionally followed by a raw Arrow
# body. This table is the single source of truth for the header shape of every
# frame: `codegen.render_schema_json()` renders it into `schema.json` (a
# JSON-Schema `oneOf` discriminated on `t` — and on `op` for REQ/PUSH — that the
# generated mirrors document) and `engine.validate_header()` enforces it at the
# trust boundary. Keep headers strictly JSON-typed (no msgpack bin/ext) so the
# decoded map validates directly.
#
# A field is `(name, type, required)` where `type` is either a JSON-Schema type
# string ("string"/"integer"/"boolean"/"object"/"array") or a full schema dict
# (for typed arrays / nullable / const values). Every variant also carries `t`
# (frame type) implicitly, and REQ/PUSH variants carry an `op` discriminator;
# both are emitted as `const` in the schema, so callers never list them here.

# Reusable field specs.
_F_ID: Any = ("id", "integer", True)
# `ns` (session/namespace routing key): rides every session-scoped frame so a
# single WebSocket can carry multiple catalog sessions. The server routes an
# inbound REQ/CANCEL/CREDIT (and a session-opening HELLO) to the handler for its
# `ns`, and stamps every response with the same `ns` so the client bridge can
# demux it back to the right pump. Optional: absent == the sole/default session,
# which is how single-session connections and every pre-mux client behave. Added
# to every variant except the connection-scoped keepalives (see _NS_EXEMPT).
_F_NS: Any = ("ns", "integer", False)
_ARRAY_OF_STR: dict[str, Any] = {"type": "array", "items": {"type": "string"}}
# Each TABLES_LIST entry is a row object ({schema, name, writable, ...}).
_ARRAY_OF_OBJ: dict[str, Any] = {"type": "array", "items": {"type": "object"}}

# variant: {"name", "t", optional "op", "fields": [field, ...], optional
#           "body": "arrow"|"arrow?"}. `body` documents that a raw Arrow body
# follows the header (and whether it is optional); it is metadata for the codec
# and docs, not part of the validated header map.
_FRAME_HEADERS: list[dict[str, Any]] = [
    # ── Handshake / lifecycle ────────────────────────────────────────────────
    {"name": "HELLO", "t": FT_HELLO, "fields": [("token", "string", False), ("catalog", "string", False)]},
    {
        "name": "HELLO_ACK",
        "t": FT_HELLO_ACK,
        "fields": [
            ("protocol_version", "integer", True),
            ("max_concurrent_reqs", "integer", True),
            ("default_batch_credits", "integer", True),
            ("session_pending", "boolean", False),
            ("capabilities", _ARRAY_OF_STR, False),
        ],
    },
    {
        "name": "HELLO_ERR",
        "t": FT_HELLO_ERR,
        "fields": [
            ("exception_type", "string", True),
            ("exception_message", "string", True),
            ("retriable", "boolean", False),
        ],
    },
    {"name": "READY", "t": FT_READY, "fields": [("capabilities", _ARRAY_OF_STR, False)]},
    {"name": "PING", "t": FT_PING, "fields": [("id", "integer", False)]},
    {"name": "PONG", "t": FT_PONG, "fields": [("id", "integer", False)]},
    # ── Control plane ────────────────────────────────────────────────────────
    {
        "name": "ERR",
        "t": FT_ERR,
        "fields": [
            _F_ID,
            ("exception_type", "string", True),
            ("exception_message", "string", True),
            ("retriable", "boolean", False),
        ],
    },
    {"name": "CANCEL", "t": FT_CANCEL, "fields": [_F_ID]},
    {"name": "CREDIT", "t": FT_CREDIT, "fields": [_F_ID, ("n", "integer", True)]},
    {
        "name": "PUSH_CATALOG_INVALIDATED",
        "t": FT_PUSH,
        "op": OP_CATALOG_INVALIDATED,
        "fields": [("schemas", _ARRAY_OF_STR, False)],
    },
    # ── Responses ────────────────────────────────────────────────────────────
    {"name": "RESP_SCHEMA", "t": FT_RESP_SCHEMA, "fields": [_F_ID], "body": "arrow"},
    {
        "name": "RESP_CHUNK_ARROW",
        "t": FT_RESP_CHUNK,
        "fields": [_F_ID, ("arrow", {"const": True}, True)],
        "body": "arrow",
    },
    {"name": "RESP_CHUNK_CATALOG", "t": FT_RESP_CHUNK, "fields": [_F_ID, ("schemas", _ARRAY_OF_STR, True)]},
    {"name": "RESP_CHUNK_TABLES", "t": FT_RESP_CHUNK, "fields": [_F_ID, ("tables", _ARRAY_OF_OBJ, True)]},
    {
        "name": "RESP_END",
        "t": FT_RESP_END,
        "fields": [
            _F_ID,
            ("rowcount", {"type": ["integer", "null"]}, False),
            ("cancelled", "boolean", False),
            ("ok", "boolean", False),
        ],
    },
    # ── Requests (t=REQ, discriminated on op) ────────────────────────────────
    {"name": "REQ_CATALOG_LIST", "t": FT_REQ, "op": OP_CATALOG_LIST, "fields": [_F_ID]},
    {"name": "REQ_TABLES_LIST", "t": FT_REQ, "op": OP_TABLES_LIST, "fields": [_F_ID, ("schema", "string", False)]},
    {
        "name": "REQ_TABLE_SCHEMA",
        "t": FT_REQ,
        "op": OP_TABLE_SCHEMA,
        "fields": [_F_ID, ("schema", "string", True), ("table", "string", True)],
    },
    {
        "name": "REQ_SCAN",
        "t": FT_REQ,
        "op": OP_SCAN,
        "fields": [
            _F_ID,
            ("schema", "string", True),
            ("table", "string", True),
            ("columns", _ARRAY_OF_STR, False),
            ("filters", "array", False),
            ("_batch_rows", "integer", False),
        ],
    },
    {
        "name": "REQ_AGGREGATE",
        "t": FT_REQ,
        "op": OP_AGGREGATE,
        "fields": [
            _F_ID,
            ("schema", "string", True),
            ("table", "string", True),
            ("columns", _ARRAY_OF_STR, False),
            ("filters", "array", False),
            ("group_by", _ARRAY_OF_STR, True),
            ("aggregates", "array", True),
            ("_batch_rows", "integer", False),
        ],
    },
    {
        "name": "REQ_INSERT",
        "t": FT_REQ,
        "op": OP_INSERT,
        "fields": [_F_ID, ("schema", "string", True), ("table", "string", True)],
        "body": "arrow",
    },
    {"name": "REQ_EXEC", "t": FT_REQ, "op": OP_EXEC, "fields": [_F_ID, ("sql", "string", True)]},
    {
        "name": "REQ_QUERY",
        "t": FT_REQ,
        "op": OP_QUERY,
        "fields": [_F_ID, ("sql", "string", True), ("_batch_rows", "integer", False)],
    },
    {
        "name": "REQ_RPC_SCALAR",
        "t": FT_REQ,
        "op": OP_RPC_SCALAR,
        "fields": [_F_ID, ("function", "string", True), ("args", "array", False)],
    },
    {
        "name": "REQ_RPC_TABLE",
        "t": FT_REQ,
        "op": OP_RPC_TABLE,
        "fields": [_F_ID, ("function", "string", True), ("args", "array", False)],
        "body": "arrow?",
    },
    {
        "name": "REQ_CREATE_TABLE",
        "t": FT_REQ,
        "op": OP_CREATE_TABLE,
        "fields": [_F_ID, ("schema", "string", True), ("name", "string", True), ("columns", "array", True)],
    },
    {
        "name": "REQ_ALTER_TABLE",
        "t": FT_REQ,
        "op": OP_ALTER_TABLE,
        "fields": [
            _F_ID,
            ("schema", "string", True),
            ("table", "string", True),
            ("kind", "string", True),
            ("details", "object", False),
        ],
    },
]


# Inject the `ns` routing key into every session-scoped variant. PING/PONG are
# connection-scoped keepalives (one per socket, never routed to a session), so
# they stay exempt. Done as a post-pass rather than repeating _F_NS in ~20 field
# lists — the mutation runs at import, before codegen/validation read the table.
_NS_EXEMPT: Final = {"PING", "PONG"}
for _variant in _FRAME_HEADERS:
    if _variant["name"] not in _NS_EXEMPT:
        _variant["fields"] = [*_variant["fields"], _F_NS]


def _field_schema(json_type: Union[str, dict[str, Any]]) -> dict[str, Any]:
    if isinstance(json_type, str):
        return {"type": json_type}
    return dict(json_type)


def header_variant_schema(variant: dict[str, Any]) -> dict[str, Any]:
    """Render one `_FRAME_HEADERS` entry into a JSON-Schema object: `t` (and
    `op`) as discriminating consts, declared fields as properties, required
    fields collected, `additionalProperties` closed so unknown keys are
    rejected (drift detection)."""
    properties: dict[str, Any] = {"t": {"const": variant["t"]}}
    required: list[str] = ["t"]
    if "op" in variant:
        properties["op"] = {"const": variant["op"]}
        required.append("op")
    for name, json_type, is_required in variant["fields"]:
        properties[name] = _field_schema(json_type)
        if is_required:
            required.append(name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


# ── Server tunables (advertised via HELLO_ACK) ───────────────────────────────
DEFAULT_BATCH_CREDITS = 8
MAX_CONCURRENT_REQS = 64

# ── Exception types (the `exception_type` field of ERR / HELLO_ERR) ──────────
#
# The closed set of values a server may put in `exception_type`, each paired with the
# HTTP status an HTTP-fronted deployment should report for it. The value names the DuckDB
# Exception subclass the client reconstructs; anything outside this set MUST be
# treated as `EXC_FALLBACK` by the client, which means an unrecognized value silently
# loses its type rather than failing loudly.
#
# This lives here because it was already maintained in three places — the prose table
# in docs/n6k-network-protocol.md, the Python server's own duckdb-class tuple, and
# the client's own reconstruction in src/n6k_client/include/n6k_err_throw.hpp — and they had
# drifted: `DependencyException` was documented and reachable but unmapped, so it went
# out as `IOException`.
#
# Note this is the *wire* spelling, which is the DuckDB class name. It is NOT what
# `Exception::ExceptionTypeToString` returns (that yields "Catalog", "TransactionContext",
# "Not implemented"); converting between the two is what src/common/n6k_exception_types.cpp
# owns, and getting it wrong is how a typed error degrades to IOException.
EXC_FALLBACK: Final = "IOException"

# (wire name, http status). 4xx is a caller fault, 5xx the server's own.
_EXCEPTION_TYPES: list[tuple[str, int]] = [
    ("BinderException", 400),
    ("ConstraintException", 400),
    ("ConversionException", 400),
    # A dependency error is the caller asking for something invalid (dropping a table
    # that others depend on), so 400. It previously reached clients as a 500
    # IOException purely because it was missing from the mapping.
    ("DependencyException", 400),
    ("InvalidInputException", 400),
    ("InvalidTypeException", 400),
    ("OutOfRangeException", 400),
    ("ParserException", 400),
    ("SyntaxException", 400),
    ("TypeMismatchException", 400),
    ("CatalogException", 404),
    ("NotImplementedException", 501),
    ("ConnectionException", 500),
    ("FatalException", 500),
    ("HTTPException", 500),
    ("InternalException", 500),
    ("InterruptException", 500),
    ("IOException", 500),
    ("OutOfMemoryException", 500),
    # 500 rather than 403: this reports a DuckDB engine-level permission failure (a
    # read-only catalog refusing a write), not a rejected caller credential.
    ("PermissionException", 500),
    ("SequenceException", 500),
    ("SerializationException", 500),
    ("TransactionException", 500),
]

# Transient faults the client may safely retry, flagged with `retriable: true`.
# Per-op cursors put each write in its own transaction, so concurrent writes to the
# same rows can lose DuckDB's optimistic-concurrency race without anything being wrong.
_RETRIABLE_EXCEPTION_TYPES: frozenset[str] = frozenset({"TransactionException"})

EXCEPTION_TYPES: Final = tuple(name for name, _ in _EXCEPTION_TYPES)
EXCEPTION_HTTP_STATUS: Final = {name: status for name, status in _EXCEPTION_TYPES}


def is_known_exception_type(name: str) -> bool:
    """Whether `name` is a wire value the client can reconstruct."""
    return name in EXCEPTION_HTTP_STATUS


def http_status_for_exception_type(name: str) -> int:
    """HTTP status for a wire exception type; 500 for anything unrecognized."""
    return EXCEPTION_HTTP_STATUS.get(name, 500)


def is_retriable_exception_type(name: str) -> bool:
    return name in _RETRIABLE_EXCEPTION_TYPES


# ── Capabilities (advertised via HELLO_ACK, and via READY for deferred
# sessions whose handler does not exist yet at HELLO_ACK time) ───────────────
# Which of these a server advertises is a per-server decision — not every server
# implements every op. The client needs the answer before it plans a query, not
# when it runs one.
CAP_AGGREGATE_PUSHDOWN: Final = "aggregate_pushdown"
