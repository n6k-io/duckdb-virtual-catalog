#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

#include <string>

namespace duckdb {

class ClientContext;

namespace n6k {

// The catalog-list contract every serve function shares: `inputs` from `first_catalog` on are
// catalog names, or the list is empty and every attached non-system catalog is served — minus the
// startup in-memory database, which a connection carries without asking for it (see the .cpp).
// Shared so n6k_serve_socket, n6k_serve_fd and n6k_serve_http reject a NULL, an unattached name and
// a repeat identically — `fn_name` only names the caller in the resulting BinderException.
vector<std::string> ResolveServedCatalogs(ClientContext &context, const vector<Value> &inputs, idx_t first_catalog,
                                          const char *fn_name);

} // namespace n6k
} // namespace duckdb
