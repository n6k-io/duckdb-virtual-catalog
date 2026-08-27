#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

// A namespace of virtual tables backed by UDFs that call into Python.
struct ProviderInfo {
	string probe_id;
	string catalog_name;
	string schema_name;
	shared_ptr<DatabaseInstance> db_instance;

	string list_udf;
	string schema_udf;
	string scan_udf;
	string insert_udf;
	string update_udf;
	string delete_udf;
	string alter_udf;

	// Bumped by vcat_invalidate_provider_tables; read only by
	// VirtualCatalogSchemaEntry::DiscardCacheIfProviderVersionMoved, which drops the cached provider
	// tables when it moves. No SQL exposes it, so a mismatch is diagnosed from the cache behaviour
	// rather than by reading the counter.
	atomic<uint64_t> version {0};

	// Set as ParentCatalog() on provider entries to route DML to the insert/update/delete UDFs.
	shared_ptr<Catalog> phantom_catalog;
};

shared_ptr<ProviderInfo> GetProvider(const string &catalog, const string &schema);
void RegisterProvider(const shared_ptr<ProviderInfo> &info);
bool BumpProviderVersion(ClientContext &context, const string &catalog, const string &schema);

void RegisterProviderFunctions(ExtensionLoader &loader);

} // namespace duckdb
