#pragma once

// A SchemaCatalogEntry that wraps a DuckSchemaEntry and union-enumerates its native entries with
// whatever the owning extension serves into the same schema. Everything here is the part both
// extensions did the same way: forwarding DDL to the wrapped schema, native lookup taking precedence
// over extension entries, and dedup by name during a scan. The extension supplies its own entries
// through the hooks below.

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "vcat_permissions.hpp"

namespace duckdb {

class AlterTableInfo;

// Private base: holds `info` so it is constructed before the SchemaCatalogEntry(catalog, info) base.
struct VirtualCatalogSchemaInfoHolder {
	CreateSchemaInfo info;
	explicit VirtualCatalogSchemaInfoHolder(const SchemaCatalogEntry &src);
};

class VirtualCatalogSchemaEntryBase : private VirtualCatalogSchemaInfoHolder, public SchemaCatalogEntry {
public:
	VirtualCatalogSchemaEntryBase(Catalog &catalog, SchemaCatalogEntry &target_schema);

	//! The DuckSchemaEntry this wrapper was built against. VirtualCatalogBase compares it by address
	//! to detect a schema that was dropped and recreated under the same name.
	SchemaCatalogEntry &TargetSchema() {
		return target_schema;
	}

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

	//! One capability row per table/view, native entries first. Name-collision precedence matches
	//! LookupEntry: native wins, and the extension only fills what is left.
	void CollectPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                        vector<TablePermissionRow> &out);

protected:
	// Rebind the transaction to the target (wrapped) catalog for DuckSchemaEntry operations.
	CatalogTransaction TargetTransaction(CatalogTransaction alias_txn);

	//! The extension's entry for `name` in this schema, or null when it serves no such name. Called
	//! only after the native lookup missed.
	virtual CatalogEntry *LookupExtensionEntry(CatalogTransaction transaction, const string &name) = 0;

	//! Emit every extension entry of `type` whose name is not already in `seen`, inserting each name
	//! it emits. `context` is null on the context-free Scan overload; an extension that cannot fill an
	//! entry without one emits nothing then rather than guessing.
	virtual void ScanExtensionEntries(optional_ptr<ClientContext> context, CatalogType type,
	                                  case_insensitive_set_t &seen,
	                                  const std::function<void(CatalogEntry &)> &callback) = 0;

	//! Throw when DROP TABLE/VIEW on `name` would hit an extension entry. Nothing here owns a native
	//! entry, so returning normally forwards the drop to the wrapped schema.
	virtual void ThrowIfExtensionOwnedOnDrop(const string &name) = 0;

	//! True when the extension handled the ALTER TABLE; false forwards it to the wrapped schema.
	virtual bool TryAlterExtensionEntry(CatalogTransaction transaction, AlterTableInfo &alter) = 0;

	//! Append one row per extension entry passing `table_filter` and not already in `seen`.
	virtual void CollectExtensionPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                                         case_insensitive_set_t &seen, vector<TablePermissionRow> &out) = 0;

	SchemaCatalogEntry &target_schema;
	Catalog &target_catalog;
};

} // namespace duckdb
