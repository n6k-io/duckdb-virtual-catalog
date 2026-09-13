#pragma once

// `ATTACH '' AS db (TYPE virtual_catalog_provider, list listfn, schema schemafn, scan scanfn)`.
// Native DuckDB entries and UDF-backed provider tables coexist in one schema; the wrapper lifecycle
// and the native side of every operation live in VirtualCatalogBase/VirtualCatalogSchemaEntryBase.
//
// list_udf answers `schema.table`, so one provider spans every schema it names and its state lives
// on the catalog rather than on a schema wrapper.

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include "vcat_catalog_base.hpp"
#include "vcat_schema_entry_base.hpp"

namespace duckdb {

struct ProviderInfo;

static constexpr const char *VIRTUAL_CATALOG_PROVIDER_TYPE = "virtual_catalog_provider";

class VirtualCatalogProvider : public VirtualCatalogBase {
public:
	explicit VirtualCatalogProvider(AttachedDatabase &db, shared_ptr<ProviderInfo> attached_provider = nullptr);
	~VirtualCatalogProvider() override;

	string GetCatalogType() override {
		return VIRTUAL_CATALOG_PROVIDER_TYPE;
	}

	static shared_ptr<StorageExtension> CreateStorageExtension();

	//! Attach (or, with null, detach) the provider serving this catalog.
	void SetProvider(shared_ptr<ProviderInfo> new_provider);

	//! One schema's slice of the provider's table list, tagged with the epoch it was listed at.
	//! `names` is shared, not copied: this is read on every catalog entry lookup.
	struct ProviderTables {
		shared_ptr<ProviderInfo> provider;
		uint64_t epoch = 0;
		shared_ptr<const vector<string>> names;
	};

	//! Bare table names the provider lists under `schema`, re-listing first if its version moved.
	ProviderTables TablesForSchema(const string &schema);

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

protected:
	unique_ptr<VirtualCatalogSchemaEntryBase> CreateSchemaWrapper(SchemaCatalogEntry &target_schema) override;

private:
	//! Call list_udf and re-split the result into schema -> tables. Never holds provider_lock across
	//! the UDF call, and returns without doing anything when the list query re-enters on this thread.
	void RefreshProviderNames();

	//! Materialise a DuckSchemaEntry for every schema the provider names: DuckCatalog only knows the
	//! schemas SQL created, and the base class needs a target schema to hand out a wrapper for.
	//! `caller` is the lookup's own transaction: a schema SQL created and committed is visible to it
	//! but not to the system transaction that creates the missing ones, which would otherwise
	//! report a write-write conflict on it instead of the IGNORE_ON_CONFLICT it asked for.
	void EnsureProviderSchemas(CatalogTransaction caller);

	mutex provider_lock;
	shared_ptr<ProviderInfo> provider;
	uint64_t listed_version = 0;
	bool names_filled = false;
	// The entry-cache generation: monotonic, unlike ProviderInfo::version, which restarts at 0 when
	// the provider is replaced. Moves only on a real invalidation, never on a plain re-list.
	uint64_t epoch = 0;
	uint64_t ensured_epoch = 0;
	bool schemas_ensured = false;
	case_insensitive_map_t<shared_ptr<const vector<string>>> tables_by_schema;
};

class VirtualCatalogProviderSchemaEntry : public VirtualCatalogSchemaEntryBase {
public:
	VirtualCatalogProviderSchemaEntry(VirtualCatalogProvider &catalog, SchemaCatalogEntry &target_schema);

protected:
	CatalogEntry *LookupExtensionEntry(CatalogTransaction transaction, const string &name) override;
	void ScanExtensionEntries(optional_ptr<ClientContext> context, CatalogType type, case_insensitive_set_t &seen,
	                          const std::function<void(CatalogEntry &)> &callback) override;
	void ThrowIfExtensionOwnedOnDrop(const string &name) override;
	bool TryAlterExtensionEntry(CatalogTransaction transaction, AlterTableInfo &alter) override;
	void CollectExtensionPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                                 case_insensitive_set_t &seen, vector<TablePermissionRow> &out) override;

private:
	//! What the provider knows this table as: `schema.table`, the shape list_udf answered in.
	string QualifiedName(const string &table_name) const;

	// Drops the entry cache when the catalog's epoch moved. Caller holds provider_lock.
	VirtualCatalogProvider::ProviderTables RefreshedTables();

	// Retire cached provider entries (keeping them alive) and empty the cache. Caller holds provider_lock.
	void RetireProviderCache();

	// Fill via schema_udf on first access. Caller holds provider_lock.
	CatalogEntry *GetOrQueryProviderEntry(const VirtualCatalogProvider::ProviderTables &tables, const string &name);

	bool TryGetProviderForTable(const string &name, shared_ptr<ProviderInfo> &out_provider);

	VirtualCatalogProvider &provider_catalog;
	mutex provider_lock;
	uint64_t cached_epoch = 0;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> provider_cache;
	// Evicted entries are retired, not freed: the binder and executor hold raw CatalogEntry*
	// mid-statement. Released at detach. Guarded by provider_lock.
	vector<unique_ptr<CatalogEntry>> retired_provider_entries;
};

} // namespace duckdb
