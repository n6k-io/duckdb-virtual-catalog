#pragma once

// The bridge's catalog: a DuckCatalog where native tables and the source's tables share a schema.
// Everything about the source is behind CrossingAttach.

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include "crossing_attach.hpp"
#include "vcat_catalog_base.hpp"
#include "vcat_schema_entry_base.hpp"

namespace duckdb {

class BridgeCatalog : public VirtualCatalogBase {
public:
	BridgeCatalog(AttachedDatabase &db, unique_ptr<CrossingSource> source);
	~BridgeCatalog() override;

	string GetCatalogType() override;

	using DuckCatalog::Initialize;
	void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override;
	void OnDetach(ClientContext &context) override;

	CrossingAttach &Attach() {
		return attach;
	}

protected:
	unique_ptr<VirtualCatalogSchemaEntryBase> CreateSchemaWrapper(SchemaCatalogEntry &target_schema) override;
	void ThrowIfSchemaStillServed(const string &schema_name) override;

private:
	CrossingAttach attach;
};

class BridgeSchemaEntry : public VirtualCatalogSchemaEntryBase {
public:
	BridgeSchemaEntry(BridgeCatalog &catalog, SchemaCatalogEntry &target_schema);

protected:
	CatalogEntry *LookupExtensionEntry(CatalogTransaction transaction, const string &name) override;
	void ScanExtensionEntries(optional_ptr<ClientContext> context, CatalogType type, case_insensitive_set_t &seen,
	                          const std::function<void(CatalogEntry &)> &callback) override;
	void ThrowIfExtensionOwnedOnDrop(const string &name) override;
	bool TryAlterExtensionEntry(CatalogTransaction transaction, AlterTableInfo &alter) override;
	void CollectExtensionPermissions(ClientContext &context, optional_ptr<const string> table_filter,
	                                 case_insensitive_set_t &seen, vector<TablePermissionRow> &out) override;

private:
	CrossingAttach &attach;
};

class BridgeTransactionManager : public DuckTransactionManager {
public:
	BridgeTransactionManager(AttachedDatabase &db, CrossingAttach &attach);

	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

private:
	CrossingAttach &attach;
};

void RegisterBridgeCatalog(ExtensionLoader &loader);

} // namespace duckdb
