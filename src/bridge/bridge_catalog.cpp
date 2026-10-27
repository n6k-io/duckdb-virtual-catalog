#include "bridge_catalog.hpp"

#include "duckdb_source.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <algorithm>

namespace duckdb {

BridgeCatalog::BridgeCatalog(AttachedDatabase &db, unique_ptr<DuckDBSource> source)
    : VirtualCatalogBase(db), attach(db, Crossing<DuckDBSource>::Adapt(std::move(source))) {
	attach.Source<DuckDBSource>().SetTarget(&attach);
}

BridgeCatalog::~BridgeCatalog() {
	attach.Source<DuckDBSource>().SetTarget(nullptr);
}

string BridgeCatalog::GetCatalogType() {
	return VIRTUAL_CATALOG_BRIDGE_TYPE;
}

void BridgeCatalog::Initialize(optional_ptr<ClientContext>, bool load_builtin) {
	DuckCatalog::Initialize(load_builtin);
	initialized = true;
	if (!IsBound()) {
		return;
	}
	// Not in the attach callback: the transaction manager does not exist until after it returns.
	CreateServedSchemas();
}

bool BridgeCatalog::IsBound() {
	return attach.Source<DuckDBSource>().IsBound();
}

void BridgeCatalog::ThrowUnlessBound() {
	if (!IsBound()) {
		throw PermissionException(
		    "virtual_catalog_bridge: not initialized; call virtual_catalog_bridge_init(bridge_id, token)");
	}
}

void BridgeCatalog::Bind(const string &bridge_id, const string &token) {
	lock_guard<mutex> guard(bind_lock);
	if (IsBound()) {
		throw PermissionException("virtual_catalog_bridge: the main database is already initialized");
	}
	auto redeemed = RedeemBridge(bridge_id, token, GetDatabase());
	auto &source = attach.Source<DuckDBSource>();
	source.Bind(*redeemed);
	source.SetTarget(&attach);
	attach.Refresh();
	CreateServedSchemas();
}

optional_ptr<CatalogEntry> BridgeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	// DuckCatalog::Initialize creates the default schema through here.
	if (initialized) {
		ThrowUnlessBound();
	}
	return VirtualCatalogBase::CreateSchema(transaction, info);
}

void BridgeCatalog::CreateServedSchemas() {
	auto transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
	for (auto &schema : attach.Schemas()) {
		CreateSchemaInfo info;
		info.catalog = GetName();
		info.schema = schema;
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		CreateSchema(transaction, info);
	}
}

// A grant made after ATTACH can serve a schema Initialize did not create.
optional_ptr<SchemaCatalogEntry> BridgeCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &schema_lookup,
                                                             OnEntryNotFound if_not_found) {
	// Unqualified function lookups walk the default catalog, so an unbound bridge answers "not here" to those.
	if (!IsBound()) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		ThrowUnlessBound();
	}
	auto entry = VirtualCatalogBase::LookupSchema(transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
	if (entry) {
		return entry;
	}
	auto served = attach.Schemas();
	auto &name = schema_lookup.GetEntryName();
	if (std::any_of(served.begin(), served.end(),
	                [&](const string &schema) { return StringUtil::CIEquals(schema, name); })) {
		CreateServedSchemas();
	}
	return VirtualCatalogBase::LookupSchema(transaction, schema_lookup, if_not_found);
}

void BridgeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	if (!IsBound()) {
		return;
	}
	CreateServedSchemas();
	VirtualCatalogBase::ScanSchemas(context, std::move(callback));
}

void BridgeCatalog::OnDetach(ClientContext &context) {
	attach.Detach(context);
	DuckCatalog::OnDetach(context);
}

PhysicalOperator &BridgeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	if (attach.DescribedSchema(op.schema.name).Allows(CrossingVerb::CREATE)) {
		return attach.PlanCreateTableAs(context, planner, op, plan);
	}
	return DuckCatalog::PlanCreateTableAs(context, planner, op, plan);
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

optional_ptr<Transaction> BridgeSchemaEntry::TransactionOf(optional_ptr<ClientContext> context) {
	return context ? Transaction::TryGet(*context, catalog.GetAttached()) : nullptr;
}

CatalogEntry *BridgeSchemaEntry::LookupExtensionEntry(CatalogTransaction transaction, const string &entry_name) {
	return attach.LookupTable(name, *this, entry_name, transaction.transaction).get();
}

void BridgeSchemaEntry::ScanExtensionEntries(optional_ptr<ClientContext> context, CatalogType type,
                                             case_insensitive_set_t &seen,
                                             const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	attach.ScanTables(name, *this, seen, callback, TransactionOf(context));
}

optional_ptr<CatalogEntry> BridgeSchemaEntry::TryCreateExtensionTable(CatalogTransaction transaction,
                                                                      BoundCreateTableInfo &info, bool &handled) {
	handled = attach.DescribedSchema(name).Allows(CrossingVerb::CREATE);
	if (!handled) {
		return nullptr;
	}
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::CREATE;
	ddl.schema = name;
	ddl.table = info.Base().table;
	ddl.create = info.base.get();
	attach.Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
	return attach.LookupTable(name, *this, ddl.table, transaction.transaction);
}

bool BridgeSchemaEntry::TryDropExtensionEntry(ClientContext &context, DropInfo &info) {
	auto &transaction = Transaction::Get(context, catalog);
	if (info.type != CatalogType::TABLE_ENTRY || !attach.ServesTable(name, info.name, &transaction)) {
		return false;
	}
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::DROP;
	ddl.schema = name;
	ddl.table = info.name;
	ddl.drop = &info;
	attach.Ddl(context, transaction, *this, ddl);
	return true;
}

void BridgeSchemaEntry::ThrowIfExtensionOwnedOnDrop(const string &entry_name) {
	attach.ThrowIfServed(name, entry_name, "DROP");
}

bool BridgeSchemaEntry::TryAlterExtensionEntry(CatalogTransaction transaction, AlterTableInfo &alter) {
	if (!attach.ServesTable(name, alter.name, transaction.transaction)) {
		return false;
	}
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::ALTER;
	ddl.schema = name;
	ddl.table = alter.name;
	ddl.alter = &alter;
	attach.Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
	return true;
}

void BridgeSchemaEntry::CollectExtensionPermissions(ClientContext &context, optional_ptr<const string> table_filter,
                                                    case_insensitive_set_t &seen, vector<TablePermissionRow> &out) {
	auto transaction = TransactionOf(&context);
	for (auto &n : attach.Tables(name, transaction)) {
		if (seen.count(n)) {
			continue;
		}
		if (table_filter && !StringUtil::CIEquals(*table_filter, n)) {
			continue;
		}
		auto described = attach.Described(name, *this, n, transaction);
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
		attach.Rollback(std::move(released));
		return error;
	}
	return attach.Commit(std::move(released));
}

void BridgeTransactionManager::RollbackTransaction(Transaction &transaction) {
	attach.Rollback(attach.Release(transaction));
	DuckTransactionManager::RollbackTransaction(transaction);
}

namespace {

unique_ptr<Catalog> AttachBridge(optional_ptr<StorageExtensionInfo>, ClientContext &context, AttachedDatabase &db,
                                 const string &, AttachInfo &info, AttachOptions &options) {
	if (options.is_main_database) {
		if (!info.path.empty() || !info.options.empty()) {
			throw IOException("virtual_catalog_bridge: open the main database as 'virtual_catalog_bridge:' with no "
			                  "path, then call virtual_catalog_bridge_init(bridge_id, token)");
		}
		return make_uniq<BridgeCatalog>(db, make_uniq<DuckDBSource>());
	}
	auto source = RedeemBridgeAttach(info, db.GetDatabase());
	info.path = string();
	return make_uniq<BridgeCatalog>(db, std::move(source));
}

unique_ptr<TransactionManager> CreateBridgeTransactionManager(optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
                                                              Catalog &catalog) {
	return make_uniq<BridgeTransactionManager>(db, CrossingAttach::Of(catalog));
}

void BridgeInitFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &db = DatabaseInstance::GetDatabase(context);
	auto count = args.size();
	if (count == 0) {
		return;
	}
	if (!context.transaction.IsAutoCommit()) {
		throw PermissionException("virtual_catalog_bridge_init: cannot run inside a transaction");
	}
	if (count > 1) {
		throw InvalidInputException("virtual_catalog_bridge_init: call it once, not once per row");
	}
	auto bridge_id = args.data[0].GetValue(0);
	auto token = args.data[1].GetValue(0);
	if (bridge_id.IsNull() || token.IsNull()) {
		throw InvalidInputException("virtual_catalog_bridge_init: bridge_id and token must not be NULL");
	}
	if (Settings::Get<LockConfigurationSetting>(DBConfig::GetConfig(db))) {
		throw PermissionException("virtual_catalog_bridge_init: the configuration is locked; call it before "
		                          "virtual_catalog_lockdown");
	}
	optional_ptr<BridgeCatalog> main;
	for (auto &attached : db.GetDatabaseManager().GetDatabases()) {
		if (attached->IsInitialDatabase() && attached->GetCatalog().GetCatalogType() == VIRTUAL_CATALOG_BRIDGE_TYPE) {
			main = &attached->GetCatalog().Cast<BridgeCatalog>();
		}
	}
	if (!main) {
		throw InvalidInputException("virtual_catalog_bridge_init: the main database is not a bridge; open it as "
		                            "'virtual_catalog_bridge:'");
	}
	main->Bind(bridge_id.ToString(), token.ToString());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ok");
}

} // namespace

void RegisterBridgeCatalog(ExtensionLoader &loader) {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = AttachBridge;
	ext->create_transaction_manager = CreateBridgeTransactionManager;
	auto &db = loader.GetDatabaseInstance();
	StorageExtension::Register(DBConfig::GetConfig(db), VIRTUAL_CATALOG_BRIDGE_TYPE, std::move(ext));
	Crossing<DuckDBSource>::RegisterPass(db);

	ScalarFunction init("virtual_catalog_bridge_init", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                    LogicalType::VARCHAR, BridgeInitFunc);
	init.stability = FunctionStability::VOLATILE;
	init.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(init);
}

} // namespace duckdb
