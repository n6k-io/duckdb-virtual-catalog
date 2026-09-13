#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <atomic>
#include <memory>

namespace duckdb {

class CatalogSession;
class N6kCatalog;
class N6kTableCatalogEntry;
struct N6kTableInfo;

class N6kSchemaEntry : public SchemaCatalogEntry {
public:
	N6kSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, string base_url, std::shared_ptr<CatalogSession> session);

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

	N6kCatalog &GetN6kCatalog();

	// Mark cache stale — lock-free, safe from the WS PUSH dispatch thread; refetch lazily in EnsureFresh().
	void Invalidate();

private:
	optional_ptr<CatalogEntry> LookupTableEntry(CatalogTransaction transaction, const string &entry_name);
	optional_ptr<CatalogEntry> GetOrCreateTableFunctionEntry(CatalogTransaction transaction, const string &entry_name);
	// Requires load_lock.
	void LoadMissingTableEntries(ClientContext &context);
	// Drain PUSH events, apply pending invalidation, ensure cache populated; called atop read paths.
	void EnsureFresh(ClientContext &context);
	// Both require tables_lock. Move the entry out of `tables` instead of destroying it -- see retired_tables.
	void RetireTableEntry(const string &entry_name);
	void RetireAllTableEntries();

	string base_url;
	std::shared_ptr<CatalogSession> session;
	// Serializes catalog refetch. Held across network I/O, so it is always acquired *before* tables_lock
	// and never while tables_lock is held.
	mutex load_lock;
	mutex tables_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> tables;
	// Entries evicted from `tables` after they may already have been handed out. duckdb's system scans keep
	// the `CatalogEntry &` from Scan() alive for the whole query (duckdb_columns stores them in its global
	// state and reads them during execution), so an evicted entry has to outlive the map slot, not the
	// lookup. Nothing reclaims these before the schema entry itself dies.
	vector<unique_ptr<CatalogEntry>> retired_tables;
	bool fetched_tables;
	std::atomic<bool> stale_ {false};
	mutex table_functions_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> table_functions;
};

} // namespace duckdb
