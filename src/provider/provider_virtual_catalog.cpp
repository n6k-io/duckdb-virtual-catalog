#include "provider_virtual_catalog.hpp"

#include "provider_arrow.hpp"
#include "provider_info.hpp"
#include "provider_table_catalog.hpp"
#include "provider_table_entry.hpp"
#include "provider_table_info.hpp"
#include "sql_build.hpp"
#include "yyjson_util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "yyjson.hpp"

namespace duckdb {

namespace {

using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;

// The list UDF runs a query, and binding that query can land back in this catalog. A re-entrant
// refresh answers from whatever is cached rather than recursing into the UDF.
struct ListingGuard {
	static idx_t &Depth() {
		static thread_local idx_t depth = 0;
		return depth;
	}
	static bool Active() {
		return Depth() > 0;
	}
	ListingGuard() {
		Depth()++;
	}
	~ListingGuard() {
		Depth()--;
	}
};

string TakeAttachOption(AttachOptions &options, const char *key) {
	auto it = options.options.find(key);
	if (it == options.options.end()) {
		return string();
	}
	auto value = StringValue::Get(it->second.DefaultCastAs(LogicalType::VARCHAR));
	// Erased, not just read: this catalog is a DuckCatalog, so whatever is left in AttachOptions
	// reaches StorageOptions::Initialize, which throws on any key it does not recognise.
	options.options.erase(it);
	return value;
}

shared_ptr<ProviderInfo> ProviderFromAttachOptions(ClientContext &context, AttachedDatabase &db,
                                                   const string &catalog_name, AttachOptions &options) {
	auto info = make_shared_ptr<ProviderInfo>();
	info->list_udf = TakeAttachOption(options, "list");
	info->schema_udf = TakeAttachOption(options, "schema");
	info->scan_udf = TakeAttachOption(options, "scan");
	info->insert_udf = TakeAttachOption(options, "insert");
	info->update_udf = TakeAttachOption(options, "update");
	info->delete_udf = TakeAttachOption(options, "delete");
	info->alter_udf = TakeAttachOption(options, "alter");

	if (info->list_udf.empty() && info->schema_udf.empty() && info->scan_udf.empty()) {
		return nullptr;
	}
	if (info->list_udf.empty() || info->schema_udf.empty() || info->scan_udf.empty()) {
		throw BinderException("virtual_catalog_provider: ATTACH takes LIST, SCHEMA and SCAN together, or none of them");
	}
	info->catalog_name = catalog_name;
	info->db_instance = context.db;
	info->phantom_catalog = make_shared_ptr<ProviderTableCatalog>(db);
	return info;
}

} // namespace

VirtualCatalogProvider::VirtualCatalogProvider(AttachedDatabase &db, shared_ptr<ProviderInfo> attached_provider)
    : VirtualCatalogBase(db), provider(std::move(attached_provider)) {
	if (provider) {
		RegisterProvider(provider);
	}
}

VirtualCatalogProvider::~VirtualCatalogProvider() {
	if (provider && GetProvider(provider->catalog_name) == provider) {
		UnregisterProvider(provider->catalog_name);
	}
}

shared_ptr<StorageExtension> VirtualCatalogProvider::CreateStorageExtension() {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &context, AttachedDatabase &db,
	                 const string &name, AttachInfo &info, AttachOptions &options) -> unique_ptr<Catalog> {
		// `ATTACH ''` is the natural spelling for a catalog with no file behind it, but the storage
		// manager built after this returns only recognises ':memory:' and would open a file named "".
		if (info.path.empty()) {
			info.path = IN_MEMORY_PATH;
		}
		return make_uniq<VirtualCatalogProvider>(db, ProviderFromAttachOptions(context, db, name, options));
	};
	ext->create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
	                                     Catalog &) -> unique_ptr<TransactionManager> {
		return make_uniq<DuckTransactionManager>(db);
	};
	return ext;
}

unique_ptr<VirtualCatalogSchemaEntryBase>
VirtualCatalogProvider::CreateSchemaWrapper(SchemaCatalogEntry &target_schema) {
	return make_uniq<VirtualCatalogProviderSchemaEntry>(*this, target_schema);
}

void VirtualCatalogProvider::SetProvider(shared_ptr<ProviderInfo> new_provider) {
	lock_guard<mutex> lock(provider_lock);
	provider = std::move(new_provider);
	tables_by_schema.clear();
	names_filled = false;
	listed_version = 0;
	schemas_ensured = false;
	epoch++;
}

void VirtualCatalogProvider::RefreshProviderNames() {
	shared_ptr<ProviderInfo> snapshot;
	uint64_t version;
	{
		lock_guard<mutex> lock(provider_lock);
		if (!provider) {
			return;
		}
		version = provider->version.load(std::memory_order_acquire);
		if (names_filled && version == listed_version) {
			return;
		}
		snapshot = provider;
	}
	if (ListingGuard::Active()) {
		return;
	}
	ListingGuard guard;

	vector<string> qualified;
	Connection conn(*snapshot->db_instance);
	auto result = conn.Query(vcat::UdfCallWithConstants(snapshot->list_udf, {}));
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_provider: list UDF failed: ");
	}
	auto chunk = result->Fetch();
	if (chunk && chunk->size() > 0) {
		auto val = chunk->GetValue(0, 0);
		if (!val.IsNull()) {
			auto listed = val.ToString();
			if (!listed.empty()) {
				qualified = StringUtil::Split(listed, '|');
			}
		}
	}

	case_insensitive_map_t<vector<string>> grouped;
	for (auto &entry : qualified) {
		auto dot = entry.find('.');
		if (dot == string::npos || dot == 0 || dot + 1 == entry.size()) {
			throw InvalidInputException(
			    "virtual_catalog_provider: list UDF returned '%s'; names must be qualified as schema.table", entry);
		}
		grouped[entry.substr(0, dot)].push_back(entry.substr(dot + 1));
	}
	case_insensitive_map_t<shared_ptr<const vector<string>>> split;
	for (auto &entry : grouped) {
		split[entry.first] = make_shared_ptr<const vector<string>>(std::move(entry.second));
	}

	lock_guard<mutex> lock(provider_lock);
	if (provider != snapshot) {
		return;
	}
	// Only a genuine version move bumps the epoch. Two callers racing to fill at the same version
	// produce the same names, and bumping would retire entries a running query holds -- the same
	// table coming back with a fresh oid halfway through one statement.
	const bool version_moved = names_filled && version != listed_version;
	tables_by_schema = std::move(split);
	listed_version = version;
	names_filled = true;
	if (version_moved) {
		epoch++;
	}
}

void VirtualCatalogProvider::EnsureProviderSchemas() {
	RefreshProviderNames();

	vector<string> schemas;
	{
		lock_guard<mutex> lock(provider_lock);
		// Every catalog lookup lands here, so the steady state is one atomic load and this check.
		if (schemas_ensured && ensured_epoch == epoch) {
			return;
		}
		for (auto &entry : tables_by_schema) {
			schemas.push_back(entry.first);
		}
		ensured_epoch = epoch;
		schemas_ensured = true;
	}
	if (schemas.empty()) {
		return;
	}
	// The system transaction, not the caller's: a SELECT reaching a schema for the first time must
	// not roll the schema's creation back with it, or join a read-only transaction that cannot write.
	auto transaction = CatalogTransaction::GetSystemTransaction(GetAttached().GetDatabase());
	for (auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema;
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		CreateSchema(transaction, info);
	}
}

VirtualCatalogProvider::ProviderTables VirtualCatalogProvider::TablesForSchema(const string &schema) {
	RefreshProviderNames();

	lock_guard<mutex> lock(provider_lock);
	ProviderTables tables;
	tables.provider = provider;
	tables.epoch = epoch;
	auto it = tables_by_schema.find(schema);
	tables.names = it != tables_by_schema.end() ? it->second : make_shared_ptr<const vector<string>>();
	return tables;
}

optional_ptr<SchemaCatalogEntry> VirtualCatalogProvider::LookupSchema(CatalogTransaction transaction,
                                                                      const EntryLookupInfo &schema_lookup,
                                                                      OnEntryNotFound if_not_found) {
	EnsureProviderSchemas();
	return VirtualCatalogBase::LookupSchema(transaction, schema_lookup, if_not_found);
}

void VirtualCatalogProvider::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	EnsureProviderSchemas();
	VirtualCatalogBase::ScanSchemas(context, std::move(callback));
}

VirtualCatalogProviderSchemaEntry::VirtualCatalogProviderSchemaEntry(VirtualCatalogProvider &catalog,
                                                                     SchemaCatalogEntry &target_schema)
    : VirtualCatalogSchemaEntryBase(catalog, target_schema), provider_catalog(catalog) {
}

string VirtualCatalogProviderSchemaEntry::QualifiedName(const string &table_name) const {
	return name + "." + table_name;
}

void VirtualCatalogProviderSchemaEntry::RetireProviderCache() {
	for (auto &kv : provider_cache) {
		if (kv.second) {
			retired_provider_entries.push_back(std::move(kv.second));
		}
	}
	provider_cache.clear();
}

VirtualCatalogProvider::ProviderTables VirtualCatalogProviderSchemaEntry::RefreshedTables() {
	auto tables = provider_catalog.TablesForSchema(name);
	if (tables.epoch != cached_epoch) {
		RetireProviderCache();
		cached_epoch = tables.epoch;
	}
	return tables;
}

CatalogEntry *
VirtualCatalogProviderSchemaEntry::GetOrQueryProviderEntry(const VirtualCatalogProvider::ProviderTables &tables,
                                                           const string &entry_name) {
	auto it = provider_cache.find(entry_name);
	if (it != provider_cache.end()) {
		return it->second.get();
	}

	auto &info = *tables.provider;
	auto qualified = QualifiedName(entry_name);
	Connection conn(*info.db_instance);
	auto result = conn.Query(vcat::UdfCallWithConstants(info.schema_udf, {Value(qualified)}));
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_provider: schema UDF failed: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return nullptr;
	}
	auto payload = chunk->GetValue(0, 0);
	if (payload.IsNull()) {
		return nullptr;
	}
	// One Arrow IPC schema message, straight from `pa.Schema.serialize()`: column names, DuckDB types
	// and the primary keys all come out of it, so the provider declares its shape once.
	auto decoded = vcat_provider::DecodeSchemaMessage(*conn.context, StringValue::Get(payload),
	                                                  "virtual_catalog_provider: schema UDF");
	auto &column_names = decoded.column_names;
	auto &column_types = decoded.column_types;

	auto table_info = make_shared_ptr<ProviderTableInfo>();
	table_info->table_name = qualified;
	table_info->column_names = column_names;
	table_info->column_types = column_types;
	table_info->primary_keys = std::move(decoded.primary_keys);
	table_info->provider = tables.provider;
	table_info->db_instance = info.db_instance;

	auto create_info = make_uniq<CreateTableInfo>(*this, entry_name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[c], column_types[c]));
	}
	auto entry = make_uniq<ProviderTableCatalogEntry>(*info.phantom_catalog, *this, *create_info, table_info);
	auto *raw = entry.get();
	provider_cache[entry_name] = std::move(entry);
	return raw;
}

bool VirtualCatalogProviderSchemaEntry::TryGetProviderForTable(const string &probe_name,
                                                               shared_ptr<ProviderInfo> &out_provider) {
	lock_guard<mutex> lock(provider_lock);
	auto tables = RefreshedTables();
	if (!tables.provider) {
		return false;
	}
	for (auto &n : *tables.names) {
		if (StringUtil::CIEquals(n, probe_name)) {
			out_provider = tables.provider;
			return true;
		}
	}
	return false;
}

CatalogEntry *VirtualCatalogProviderSchemaEntry::LookupExtensionEntry(CatalogTransaction /*transaction*/,
                                                                      const string &entry_name) {
	lock_guard<mutex> lock(provider_lock);
	auto tables = RefreshedTables();
	if (!tables.provider) {
		return nullptr;
	}
	for (auto &n : *tables.names) {
		if (StringUtil::CIEquals(n, entry_name)) {
			return GetOrQueryProviderEntry(tables, n);
		}
	}
	return nullptr;
}

void VirtualCatalogProviderSchemaEntry::ScanExtensionEntries(optional_ptr<ClientContext> /*context*/,
                                                             CatalogType /*type*/, case_insensitive_set_t &seen,
                                                             const std::function<void(CatalogEntry &)> &callback) {
	// No ClientContext needed: a provider entry is filled by querying the UDF on the provider's own
	// DatabaseInstance, so both Scan overloads report the same set.
	lock_guard<mutex> lock(provider_lock);
	auto tables = RefreshedTables();
	if (!tables.provider) {
		return;
	}
	for (auto &n : *tables.names) {
		if (seen.count(n)) {
			continue;
		}
		auto *entry = GetOrQueryProviderEntry(tables, n);
		if (entry) {
			callback(*entry);
			seen.insert(n);
		}
	}
}

void VirtualCatalogProviderSchemaEntry::ThrowIfExtensionOwnedOnDrop(const string &entry_name) {
	shared_ptr<ProviderInfo> owning;
	if (TryGetProviderForTable(entry_name, owning)) {
		throw BinderException("Table '%s' is provider-managed; modify the backing store and call "
		                      "provider_invalidate_tables() instead of DROP TABLE",
		                      entry_name);
	}
}

bool VirtualCatalogProviderSchemaEntry::TryAlterExtensionEntry(CatalogTransaction /*transaction*/,
                                                               AlterTableInfo &alter) {
	shared_ptr<ProviderInfo> owning;
	if (!TryGetProviderForTable(alter.name, owning)) {
		return false;
	}

	// Empty alter_udf means the provider did not implement alter(); not editable.
	if (owning->alter_udf.empty()) {
		throw PermissionException("Provider table '%s' does not support ALTER", alter.name);
	}

	string kind;
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	switch (alter.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter.Cast<AddColumnInfo>();
		kind = "add_column";
		const auto &column_name = add.new_column.Name();
		auto type_str = add.new_column.Type().ToString();
		yyjson_mut_obj_add_strncpy(doc, root, "name", column_name.c_str(), column_name.size());
		yyjson_mut_obj_add_strncpy(doc, root, "type", type_str.c_str(), type_str.size());
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &rem = alter.Cast<RemoveColumnInfo>();
		kind = "drop_column";
		yyjson_mut_obj_add_strncpy(doc, root, "name", rem.removed_column.c_str(), rem.removed_column.size());
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &ren = alter.Cast<RenameColumnInfo>();
		kind = "rename_column";
		yyjson_mut_obj_add_strncpy(doc, root, "old_name", ren.old_name.c_str(), ren.old_name.size());
		yyjson_mut_obj_add_strncpy(doc, root, "new_name", ren.new_name.c_str(), ren.new_name.size());
		break;
	}
	default:
		yyjson_mut_doc_free(doc);
		throw BinderException("ALTER TABLE %s on provider-managed tables is not supported "
		                      "(only ADD COLUMN, DROP COLUMN, RENAME COLUMN)",
		                      alter.ToString());
	}
	auto details_json = vcat::SerializeJsonDocAndFree(doc);

	Connection udf_conn(*owning->db_instance);
	auto udf_result = udf_conn.Query(vcat::UdfCallWithConstants(
	    owning->alter_udf, {Value(QualifiedName(alter.name)), Value(kind), Value(details_json)}));
	if (udf_result->HasError()) {
		udf_result->GetErrorObject().Throw("virtual_catalog_provider: alter UDF failed: ");
	}

	// Evict only this table's cached entry; retire (don't free) it since a concurrent scan may hold its pointer.
	lock_guard<mutex> lock(provider_lock);
	auto it = provider_cache.find(alter.name);
	if (it != provider_cache.end()) {
		if (it->second) {
			retired_provider_entries.push_back(std::move(it->second));
		}
		provider_cache.erase(it);
	}
	return true;
}

void VirtualCatalogProviderSchemaEntry::CollectExtensionPermissions(ClientContext & /*context*/,
                                                                    optional_ptr<const string> table_filter,
                                                                    case_insensitive_set_t &seen,
                                                                    vector<TablePermissionRow> &out) {
	// An empty UDF name means the provider did not implement that operation.
	lock_guard<mutex> lock(provider_lock);
	auto tables = RefreshedTables();
	if (!tables.provider) {
		return;
	}
	auto &info = *tables.provider;
	vector<string> provider_verbs {"select"};
	if (!info.insert_udf.empty()) {
		provider_verbs.push_back("insert");
	}
	if (!info.update_udf.empty()) {
		provider_verbs.push_back("update");
	}
	if (!info.delete_udf.empty()) {
		provider_verbs.push_back("delete");
	}
	if (!info.alter_udf.empty()) {
		provider_verbs.push_back("alter");
	}
	for (auto &n : *tables.names) {
		if ((table_filter && !StringUtil::CIEquals(n, *table_filter)) || seen.count(n)) {
			continue;
		}
		seen.insert(n);
		vector<string> pk;
		auto *entry = GetOrQueryProviderEntry(tables, n);
		if (entry) {
			pk = entry->Cast<ProviderTableCatalogEntry>().table_info->primary_keys;
		}
		out.push_back({name, n, "provider", std::move(pk), provider_verbs});
	}
}

} // namespace duckdb
