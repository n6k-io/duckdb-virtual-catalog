#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "provider_info.hpp"

namespace duckdb {

// Per-table state built from a Provider.schema(name) callback, cached until the version counter bumps.
struct ProviderTableInfo {
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> primary_keys;

	shared_ptr<ProviderInfo> provider;
	shared_ptr<DatabaseInstance> db_instance;
};

} // namespace duckdb
