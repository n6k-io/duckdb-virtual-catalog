#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_hello_ns_distinct()`, a loopback-only diagnostic table function that pins the FT_HELLO
// a native attach puts on the wire: it must carry a nonzero `ns`, and successive attaches must carry
// distinct ones. A multiplexed serve refuses a HELLO that names no session, and PackHello omits the
// key entirely when ns == 0 — so an unset ns is indistinguishable on
// the wire from a pre-mux client. Drives the real CatalogSession::Create against an in-process
// ix::WebSocketServer that answers HELLO_ACK, and decodes the captured HELLO. Emits one row.
void RegisterN6kSelftestHelloNsDistinct(ExtensionLoader &loader);
} // namespace duckdb
