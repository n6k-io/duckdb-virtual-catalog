#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "bridge_table_entry.hpp"

namespace duckdb {

struct BridgeDMLContext {
	unique_ptr<Connection> conn;
	string quoted_table;
	const vector<string> &pk_cols;
};

BridgeDMLContext OpenAuthorizedSourceWriteContext(BridgeTableCatalogEntry &table);

} // namespace duckdb
