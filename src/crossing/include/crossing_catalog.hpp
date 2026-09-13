#pragma once

// The catalog that serves one CrossingSource. `ATTACH '<path>' AS x (TYPE <type>)` builds one per
// attach, with `type` and the factory both fixed at CrossingSource::Register.
//
// Modelled on src/provider rather than the old bridge in src/bridge.old: the catalog itself holds
// no source database, no source Connection and no source Binder -- all of that is behind
// CrossingSource. Unlike the provider a source is per-catalog rather than per-schema -- one ATTACH is one source -- so
// the source lives on the catalog and the schema wrappers reach it through ParentCatalog().

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include "crossing.hpp"
#include "vcat_catalog_base.hpp"
#include "vcat_schema_entry_base.hpp"

namespace duckdb {

class CrossingWriteCatalog;

class CrossingCatalog : public VirtualCatalogBase {
public:
	CrossingCatalog(AttachedDatabase &db, string catalog_type, unique_ptr<CrossingSource> source);
	~CrossingCatalog() override;

	string GetCatalogType() override {
		return catalog_type;
	}

	using DuckCatalog::Initialize;
	void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override;

	optional_ptr<CrossingSource> FindSourceFor(const string &schema, const string &table);
	//! Sorted.
	vector<string> TablesIn(const string &schema);

	//! Where DML on this catalog's tables is routed. Entries name it as their ParentCatalog so
	//! DuckDB's planner reaches PlanInsert/PlanUpdate/PlanDelete.
	Catalog &WriteCatalog();

protected:
	unique_ptr<VirtualCatalogSchemaEntryBase> CreateSchemaWrapper(SchemaCatalogEntry &target_schema) override;
	void ThrowIfSchemaStillServed(const string &schema_name) override;

private:
	string catalog_type;
	unique_ptr<CrossingSource> source;
	//! Read off the source once, at attach.
	case_insensitive_map_t<case_insensitive_set_t> served;
	shared_ptr<CrossingWriteCatalog> phantom;
};

class CrossingSchemaEntry : public VirtualCatalogSchemaEntryBase {
public:
	CrossingSchemaEntry(Catalog &catalog, SchemaCatalogEntry &target_schema);

protected:
	CatalogEntry *LookupExtensionEntry(CatalogTransaction transaction, const string &name) override;
	void ScanExtensionEntries(optional_ptr<ClientContext> context, CatalogType type, case_insensitive_set_t &seen,
	                          const std::function<void(CatalogEntry &)> &callback) override;
	void ThrowIfExtensionOwnedOnDrop(const string &name) override;
	bool TryAlterExtensionEntry(CatalogTransaction transaction, AlterTableInfo &alter) override;
	void CollectExtensionPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                                 case_insensitive_set_t &seen, vector<TablePermissionRow> &out) override;

private:
	CrossingCatalog &ParentCrossingCatalog();
	//! Caller holds source_lock.
	const vector<string> &GetOrAskForTableNames();
	//! Caller holds source_lock. Describe on first use; cached until the route changes.
	CatalogEntry *GetOrDescribeEntry(const string &name, CrossingSource &source);
	bool ServesTable(const string &name);

	mutex source_lock;
	vector<string> cached_names;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> cache;
	//! Never freed while the attach lives: a binder can hold a raw CatalogEntry* for the rest of
	//! the statement. Same reason the provider retires its own.
	vector<unique_ptr<CatalogEntry>> retired;
};

} // namespace duckdb
