#pragma once

#include "duckdb.hpp"
#include "provider_info.hpp"

namespace duckdb {

// Per-table state built from a Provider.schema(name) callback, cached until the version counter bumps.
struct ProviderTableInfo {
	string table_name;
	vector<string> primary_keys;

	shared_ptr<ProviderInfo> provider;
};

} // namespace duckdb
