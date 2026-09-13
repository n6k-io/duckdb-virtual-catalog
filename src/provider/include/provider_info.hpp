#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

// A catalog of virtual tables backed by UDFs that call into the host. list_udf names them
// `schema.table`, and every other UDF is called with that same qualified name.
struct ProviderInfo {
	string catalog_name;
	shared_ptr<DatabaseInstance> db_instance;

	string list_udf;
	string schema_udf;
	string scan_udf;
	string insert_udf;
	string update_udf;
	string delete_udf;
	string alter_udf;

	// Bumped by provider_invalidate_tables; read only by VirtualCatalogProvider::RefreshProviderNames.
	// No SQL exposes it.
	atomic<uint64_t> version {0};

	// Set as ParentCatalog() on provider entries to route DML to the insert/update/delete UDFs.
	shared_ptr<Catalog> phantom_catalog;
};

shared_ptr<ProviderInfo> GetProvider(const string &catalog);
void RegisterProvider(const shared_ptr<ProviderInfo> &info);
bool UnregisterProvider(const string &catalog);
bool BumpProviderVersion(const string &catalog);

void RegisterProviderFunctions(ExtensionLoader &loader);

} // namespace duckdb
