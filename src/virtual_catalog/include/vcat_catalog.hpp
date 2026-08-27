#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class VirtualCatalogSchemaEntry;

// Catalog for `ATTACH ':memory:' (TYPE virtual_catalog)`: wraps each DuckSchemaEntry in an
// VirtualCatalogSchemaEntry so provider + bridge entries coexist with native ones in the same schema.
class VirtualCatalog : public DuckCatalog {
public:
	explicit VirtualCatalog(AttachedDatabase &db);
	~VirtualCatalog() override;

	string GetCatalogType() override {
		return "virtual_catalog";
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	static shared_ptr<StorageExtension> CreateStorageExtension();

private:
	VirtualCatalogSchemaEntry &GetOrCreateWrapper(SchemaCatalogEntry &target_schema);

	mutex wrappers_lock;
	unordered_map<string, unique_ptr<VirtualCatalogSchemaEntry>> wrappers;
};

} // namespace duckdb
