#include "bridge_catalog.hpp"

#include "duckdb_source.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

BridgeCatalog::BridgeCatalog(AttachedDatabase &db, unique_ptr<DuckDBSource> source)
    : VirtualCatalogBase(db), attach(db, Crossing<DuckDBSource>::Adapt(std::move(source))) {
}

BridgeCatalog::~BridgeCatalog() = default;

string BridgeCatalog::GetCatalogType() {
	return VIRTUAL_CATALOG_BRIDGE_TYPE;
}

void BridgeCatalog::Initialize(optional_ptr<ClientContext>, bool load_builtin) {
	DuckCatalog::Initialize(load_builtin);
	// Not in the attach callback: the transaction manager does not exist until after it returns.
	auto transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
	for (auto &schema : attach.Schemas()) {
		CreateSchemaInfo info;
		info.catalog = GetName();
		info.schema = schema;
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		CreateSchema(transaction, info);
	}
}

void BridgeCatalog::OnDetach(ClientContext &context) {
	attach.Detach(context);
	DuckCatalog::OnDetach(context);
}

unique_ptr<VirtualCatalogSchemaEntryBase> BridgeCatalog::CreateSchemaWrapper(SchemaCatalogEntry &target_schema) {
	return make_uniq<BridgeSchemaEntry>(*this, target_schema);
}

void BridgeCatalog::ThrowIfSchemaStillServed(const string &schema_name) {
	attach.ThrowIfSchemaServed(schema_name);
}

BridgeSchemaEntry::BridgeSchemaEntry(BridgeCatalog &catalog, SchemaCatalogEntry &target_schema)
    : VirtualCatalogSchemaEntryBase(catalog, target_schema), attach(catalog.Attach()) {
}

CatalogEntry *BridgeSchemaEntry::LookupExtensionEntry(CatalogTransaction, const string &entry_name) {
	return attach.LookupTable(name, *this, entry_name).get();
}

void BridgeSchemaEntry::ScanExtensionEntries(optional_ptr<ClientContext>, CatalogType type,
                                             case_insensitive_set_t &seen,
                                             const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	attach.ScanTables(name, *this, seen, callback);
}

void BridgeSchemaEntry::ThrowIfExtensionOwnedOnDrop(const string &entry_name) {
	attach.ThrowIfServed(name, entry_name, "DROP");
}

bool BridgeSchemaEntry::TryAlterExtensionEntry(CatalogTransaction, AlterTableInfo &alter) {
	if (!attach.ServesTable(name, alter.name)) {
		return false;
	}
	attach.ThrowIfServed(name, alter.name, "ALTER TABLE");
	return true;
}

void BridgeSchemaEntry::CollectExtensionPermissions(ClientContext &, optional_ptr<const string> table_filter,
                                                    case_insensitive_set_t &seen, vector<TablePermissionRow> &out) {
	for (auto &n : attach.Tables(name)) {
		if (seen.count(n)) {
			continue;
		}
		if (table_filter && !StringUtil::CIEquals(*table_filter, n)) {
			continue;
		}
		auto described = attach.Described(name, *this, n);
		if (!described) {
			continue;
		}
		TablePermissionRow row;
		row.schema = name;
		row.name = n;
		row.kind = "crossing";
		row.primary_key = described->key;
		for (auto verb : CrossingVerbs()) {
			if (described->Allows(verb)) {
				row.verbs.emplace_back(CrossingVerbName(verb));
			}
		}
		out.push_back(std::move(row));
		seen.insert(n);
	}
}

BridgeTransactionManager::BridgeTransactionManager(AttachedDatabase &db, CrossingAttach &attach_p)
    : DuckTransactionManager(db), attach(attach_p) {
}

ErrorData BridgeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto released = attach.Release(transaction);
	auto error = DuckTransactionManager::CommitTransaction(context, transaction);
	if (error.HasError()) {
		CrossingAttach::Rollback(std::move(released));
		return error;
	}
	return CrossingAttach::Commit(std::move(released));
}

void BridgeTransactionManager::RollbackTransaction(Transaction &transaction) {
	CrossingAttach::Rollback(attach.Release(transaction));
	DuckTransactionManager::RollbackTransaction(transaction);
}

namespace {

unique_ptr<Catalog> AttachBridge(optional_ptr<StorageExtensionInfo>, ClientContext &context, AttachedDatabase &db,
                                 const string &, AttachInfo &info, AttachOptions &) {
	auto source = RedeemBridgeAttach(context, info);
	info.path = string();
	return make_uniq<BridgeCatalog>(db, std::move(source));
}

unique_ptr<TransactionManager> CreateBridgeTransactionManager(optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
                                                              Catalog &catalog) {
	return make_uniq<BridgeTransactionManager>(db, CrossingAttach::Of(catalog));
}

} // namespace

void RegisterBridgeCatalog(ExtensionLoader &loader) {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = AttachBridge;
	ext->create_transaction_manager = CreateBridgeTransactionManager;
	auto &db = loader.GetDatabaseInstance();
	StorageExtension::Register(DBConfig::GetConfig(db), VIRTUAL_CATALOG_BRIDGE_TYPE, std::move(ext));
	Crossing<DuckDBSource>::RegisterPass(db);
}

} // namespace duckdb
