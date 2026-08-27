#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "vcat_permissions.hpp"

namespace duckdb {

class VirtualCatalog;
struct ProviderInfo;
struct BridgeInfo;

// Private base: holds `info` so it is constructed before the SchemaCatalogEntry(catalog, info) base.
struct VirtualCatalogSchemaInfoHolder {
	CreateSchemaInfo info;
	explicit VirtualCatalogSchemaInfoHolder(const SchemaCatalogEntry &src);
};

// Wraps a DuckSchemaEntry, union-enumerating native entries with lazily-filled provider/bridge
// entries.
class VirtualCatalogSchemaEntry : private VirtualCatalogSchemaInfoHolder, public SchemaCatalogEntry {
public:
	VirtualCatalogSchemaEntry(Catalog &catalog, SchemaCatalogEntry &target_schema);

	void SetProvider(shared_ptr<ProviderInfo> provider);

	void SetBridge(shared_ptr<BridgeInfo> bridge, shared_ptr<Catalog> phantom);

	//! The bridge_id currently bound here, or "" if none. Lets vcat_unregister_bridge take the same
	//! (catalog, schema) arguments as vcat_unregister_provider instead of making the caller remember
	//! which id it used at setup.
	string CurrentBridgeId();

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	// Appends a capability row per table/view without triggering lazy fills (no source/UDF calls).
	// Name-collision precedence: native > provider > bridge.
	void CollectPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                        vector<TablePermissionRow> &out);

private:
	// Rebind the transaction to the target (wrapped) catalog for DuckSchemaEntry operations.
	CatalogTransaction TargetTransaction(CatalogTransaction alias_txn);

	// Caller holds provider_lock.
	void DiscardCacheIfProviderVersionMoved();

	// Retire cached provider entries (keeping them alive) and empty the cache. Caller holds provider_lock.
	void RetireProviderCache();

	// Caller holds provider_lock.
	const vector<string> &GetOrQueryProviderTableNames();

	// Fill via schema_udf on first access. Caller holds provider_lock.
	CatalogEntry *GetOrQueryProviderEntry(const string &name);

	// Fill on first access (ViewCatalogEntry for READ, BridgeTableCatalogEntry for READWRITE). Caller holds
	// bridge_lock.
	CatalogEntry *GetOrFillBridgeEntry(ClientContext &context, const string &name);

	bool TryGetProviderForTable(const string &name, shared_ptr<ProviderInfo> &out_provider);
	bool IsBridgeOwned(const string &name);

	SchemaCatalogEntry &target_schema;
	Catalog &target_catalog;

	// Provider state, guarded by provider_lock.
	mutex provider_lock;
	shared_ptr<ProviderInfo> provider;
	uint64_t cached_version = 0;
	vector<string> cached_names;
	bool names_filled = false;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> provider_cache;
	// Evicted entries are retired here (not freed) since binder/executor hold raw CatalogEntry* mid-statement;
	// released at detach. Guarded by provider_lock.
	vector<unique_ptr<CatalogEntry>> retired_provider_entries;

	// Bridge state, guarded by bridge_lock.
	mutex bridge_lock;
	shared_ptr<BridgeInfo> bridge_info;
	shared_ptr<Catalog> bridge_phantom;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> bridge_cache;
};

} // namespace duckdb
