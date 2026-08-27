#include "vcat_schema_entry.hpp"
#include "sql_escape.hpp"
#include "yyjson_util.hpp"
#include "bridge_bridge.hpp"
#include "bridge_table_entry.hpp"
#include "provider_arrow.hpp"
#include "provider_info.hpp"
#include "provider_table_entry.hpp"
#include "provider_table_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "yyjson.hpp"

namespace duckdb {

namespace {

using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_write;

} // namespace

VirtualCatalogSchemaInfoHolder::VirtualCatalogSchemaInfoHolder(const SchemaCatalogEntry &src) {
	info.schema = src.name;
	info.internal = src.internal;
	info.comment = src.comment;
	info.tags = src.tags;
}

VirtualCatalogSchemaEntry::VirtualCatalogSchemaEntry(Catalog &catalog, SchemaCatalogEntry &target_schema_p)
    : VirtualCatalogSchemaInfoHolder(target_schema_p), SchemaCatalogEntry(catalog, info),
      target_schema(target_schema_p), target_catalog(target_schema_p.ParentCatalog()) {
}

CatalogTransaction VirtualCatalogSchemaEntry::TargetTransaction(CatalogTransaction alias_txn) {
	return CatalogTransaction(target_catalog, alias_txn.GetContext());
}

void VirtualCatalogSchemaEntry::SetProvider(shared_ptr<ProviderInfo> new_provider) {
	lock_guard<mutex> lock(provider_lock);
	provider = std::move(new_provider);
	RetireProviderCache();
	cached_names.clear();
	names_filled = false;
	cached_version = 0;
}

void VirtualCatalogSchemaEntry::SetBridge(shared_ptr<BridgeInfo> new_bridge, shared_ptr<Catalog> phantom) {
	lock_guard<mutex> lock(bridge_lock);
	bridge_info = std::move(new_bridge);
	bridge_phantom = std::move(phantom);
	bridge_cache.clear();
}

string VirtualCatalogSchemaEntry::CurrentBridgeId() {
	lock_guard<mutex> lock(bridge_lock);
	return bridge_info ? bridge_info->bridge_id : string();
}

CatalogEntry *VirtualCatalogSchemaEntry::GetOrFillBridgeEntry(ClientContext &context, const string &entry_name) {
	auto it = bridge_cache.find(entry_name);
	if (it != bridge_cache.end()) {
		return it->second.get();
	}
	if (!bridge_info) {
		return nullptr;
	}

	TablePermission perm;
	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasAnyPermission(entry_name)) {
			return nullptr;
		}
		perm = bridge_info->permissions.at(entry_name);
	}

	if (perm == TablePermission::READ) {
		auto result = make_uniq<CreateViewInfo>();
		result->schema = name;
		result->view_name = entry_name;
		result->sql = StringUtil::Format("SELECT * FROM vcat_scan(%s, %s)", SQLString(bridge_info->bridge_id),
		                                 SQLString(entry_name));
		auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
		auto entry = make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, *this, *view_info);
		auto *ptr = entry.get();
		bridge_cache[entry_name] = std::move(entry);
		return ptr;
	}

	// READWRITE: bridge_phantom is ParentCatalog() so DML routes to bridge operators.
	auto source_conn = make_uniq<Connection>(*bridge_info->source_db);
	auto qualified_table = bridge_info->QualifiedSourceTable(entry_name);
	auto schema_result = source_conn->SendQuery("SELECT * FROM " + qualified_table + " LIMIT 0");
	if (schema_result->HasError()) {
		throw IOException("virtual_catalog: failed to get schema for '%s' on source: %s", entry_name,
		                  schema_result->GetError());
	}

	auto schema_wrapper = make_uniq<ResultArrowArrayStreamWrapper>(std::move(schema_result), 1);
	ArrowSchema arrow_schema;
	schema_wrapper->stream.get_schema(&schema_wrapper->stream, &arrow_schema);

	auto create_info = make_uniq<CreateTableInfo>(*this, entry_name);
	for (int i = 0; i < arrow_schema.n_children; i++) {
		auto *child = arrow_schema.children[i];
		string col_name = child->name;
		auto arrow_type = ArrowType::GetArrowLogicalType(context, *child);
		create_info->columns.AddColumn(ColumnDefinition(col_name, arrow_type->GetDuckType(true)));
	}
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}

	auto table_entry =
	    make_uniq<BridgeTableCatalogEntry>(*bridge_phantom, *this, *create_info, bridge_info, entry_name);
	auto *ptr = table_entry.get();
	bridge_cache[entry_name] = std::move(table_entry);
	return ptr;
}

void VirtualCatalogSchemaEntry::RetireProviderCache() {
	for (auto &kv : provider_cache) {
		if (kv.second) {
			retired_provider_entries.push_back(std::move(kv.second));
		}
	}
	provider_cache.clear();
}

void VirtualCatalogSchemaEntry::DiscardCacheIfProviderVersionMoved() {
	if (!provider) {
		return;
	}
	auto current = provider->version.load(std::memory_order_acquire);
	if (current != cached_version) {
		RetireProviderCache();
		cached_names.clear();
		names_filled = false;
		cached_version = current;
	}
}

const vector<string> &VirtualCatalogSchemaEntry::GetOrQueryProviderTableNames() {
	DiscardCacheIfProviderVersionMoved();
	if (names_filled || !provider) {
		return cached_names;
	}
	Connection conn(*provider->db_instance);
	auto sql = "SELECT " + KeywordHelper::WriteOptionallyQuoted(provider->list_udf) + "()";
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->GetErrorObject().Throw("vcat_provider: list UDF failed: ");
	}
	auto chunk = result->Fetch();
	if (chunk && chunk->size() > 0) {
		auto val = chunk->GetValue(0, 0);
		if (!val.IsNull()) {
			auto s = val.ToString();
			if (!s.empty()) {
				cached_names = StringUtil::Split(s, '|');
			}
		}
	}
	names_filled = true;
	return cached_names;
}

CatalogEntry *VirtualCatalogSchemaEntry::GetOrQueryProviderEntry(const string &name) {
	auto it = provider_cache.find(name);
	if (it != provider_cache.end()) {
		return it->second.get();
	}
	if (!provider) {
		return nullptr;
	}

	Connection conn(*provider->db_instance);
	auto sql = "SELECT " + KeywordHelper::WriteOptionallyQuoted(provider->schema_udf) + "(" +
	           vcat::QuoteSqlLiteral(name) + ")";
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->GetErrorObject().Throw("vcat_provider: schema UDF failed: ");
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
	// and the primary keys all come out of it, so the provider declares its shape once, in Arrow's own
	// vocabulary, rather than in a text format only these two files understand.
	auto decoded =
	    vcat_provider::DecodeSchemaMessage(*conn.context, StringValue::Get(payload), "vcat_provider: schema UDF");
	auto &column_names = decoded.column_names;
	auto &column_types = decoded.column_types;

	auto table_info = make_shared_ptr<ProviderTableInfo>();
	table_info->table_name = name;
	table_info->column_names = column_names;
	table_info->column_types = column_types;
	table_info->primary_keys = std::move(decoded.primary_keys);
	table_info->provider = provider;
	table_info->db_instance = provider->db_instance;
	table_info->version_at_fill = cached_version;

	auto create_info = make_uniq<CreateTableInfo>(*this, name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[c], column_types[c]));
	}
	auto entry = make_uniq<ProviderTableCatalogEntry>(*provider->phantom_catalog, *this, *create_info, table_info);
	auto *raw = entry.get();
	provider_cache[name] = std::move(entry);
	return raw;
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                                  BoundCreateTableInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateTable(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                     CreateFunctionInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateFunction(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateView(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                  TableCatalogEntry &table) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateIndex(txn, info, table);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                     CreateSequenceInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateSequence(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                          CreateTableFunctionInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateTableFunction(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                         CreateCopyFunctionInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateCopyFunction(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                           CreatePragmaFunctionInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreatePragmaFunction(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                      CreateCollationInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateCollation(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	auto txn = TargetTransaction(transaction);
	return target_schema.CreateType(txn, info);
}

optional_ptr<CatalogEntry> VirtualCatalogSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &lookup_info) {
	// Precedence on name collisions: native > provider > bridge.
	auto txn = TargetTransaction(transaction);
	auto native = target_schema.LookupEntry(txn, lookup_info);
	if (native) {
		return native;
	}

	auto entry_type = lookup_info.GetCatalogType();
	if (entry_type != CatalogType::TABLE_ENTRY && entry_type != CatalogType::VIEW_ENTRY) {
		return nullptr;
	}
	auto &lookup_name = lookup_info.GetEntryName();

	{
		lock_guard<mutex> lock(provider_lock);
		if (provider) {
			const auto &names = GetOrQueryProviderTableNames();
			for (auto &n : names) {
				if (StringUtil::CIEquals(n, lookup_name)) {
					return GetOrQueryProviderEntry(lookup_name);
				}
			}
		}
	}

	if (transaction.HasContext()) {
		lock_guard<mutex> lock(bridge_lock);
		if (bridge_info) {
			auto *e = GetOrFillBridgeEntry(transaction.GetContext(), lookup_name);
			if (e) {
				return e;
			}
		}
	}

	return nullptr;
}

bool VirtualCatalogSchemaEntry::TryGetProviderForTable(const string &probe_name,
                                                       shared_ptr<ProviderInfo> &out_provider) {
	lock_guard<mutex> lock(provider_lock);
	if (!provider) {
		return false;
	}
	for (auto &n : GetOrQueryProviderTableNames()) {
		if (StringUtil::CIEquals(n, probe_name)) {
			out_provider = provider;
			return true;
		}
	}
	return false;
}

bool VirtualCatalogSchemaEntry::IsBridgeOwned(const string &probe_name) {
	lock_guard<mutex> lock(bridge_lock);
	if (!bridge_info) {
		return false;
	}
	lock_guard<mutex> bl(bridge_info->mtx);
	return bridge_info->HasAnyPermission(probe_name);
}

void VirtualCatalogSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::VIEW_ENTRY) {
		shared_ptr<ProviderInfo> p;
		if (TryGetProviderForTable(info.name, p)) {
			throw BinderException("Table '%s' is provider-managed; modify the backing store and call "
			                      "invalidate_provider_tables() instead of DROP TABLE",
			                      info.name);
		}
		if (IsBridgeOwned(info.name)) {
			throw BinderException("Table '%s' is bridge-managed; DROP is not supported — detach the bridge instead",
			                      info.name);
		}
	}
	target_schema.DropEntry(context, info);
}

void VirtualCatalogSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	// Only ALTER_TABLE can hit a provider/bridge entry; other kinds forward to native.
	if (info.type != AlterType::ALTER_TABLE) {
		auto txn = TargetTransaction(transaction);
		target_schema.Alter(txn, info);
		return;
	}
	auto &alter = info.Cast<AlterTableInfo>();

	if (IsBridgeOwned(alter.name)) {
		throw BinderException("Table '%s' is bridge-managed; ALTER TABLE is not supported", alter.name);
	}

	shared_ptr<ProviderInfo> p;
	if (!TryGetProviderForTable(alter.name, p)) {
		auto txn = TargetTransaction(transaction);
		target_schema.Alter(txn, info);
		return;
	}

	// Empty alter_udf means the provider did not implement alter(); not editable.
	if (p->alter_udf.empty()) {
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
		const auto &name = add.new_column.Name();
		auto type_str = add.new_column.Type().ToString();
		yyjson_mut_obj_add_strncpy(doc, root, "name", name.c_str(), name.size());
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

	Connection udf_conn(*p->db_instance);
	// details_json embeds user column names, so it needs the same quoting alter.name already had.
	auto udf_sql = "SELECT " + KeywordHelper::WriteOptionallyQuoted(p->alter_udf) + "(" +
	               vcat::QuoteSqlLiteral(alter.name) + ", " + vcat::QuoteSqlLiteral(kind) + ", " +
	               vcat::QuoteSqlLiteral(details_json) + ")";
	auto udf_result = udf_conn.Query(udf_sql);
	if (udf_result->HasError()) {
		udf_result->GetErrorObject().Throw("vcat_provider: alter UDF failed: ");
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
}

static case_insensitive_set_t CollectNativeNames(SchemaCatalogEntry &target, ClientContext *context, CatalogType type) {
	case_insensitive_set_t out;
	auto visit = [&](CatalogEntry &e) {
		out.insert(e.name);
	};
	if (context) {
		target.Scan(*context, type, visit);
	} else {
		target.Scan(type, visit);
	}
	return out;
}

void VirtualCatalogSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                     const std::function<void(CatalogEntry &)> &callback) {
	target_schema.Scan(context, type, callback);

	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return;
	}
	auto seen = CollectNativeNames(target_schema, &context, type);

	{
		lock_guard<mutex> lock(provider_lock);
		if (provider) {
			for (auto &n : GetOrQueryProviderTableNames()) {
				if (seen.count(n)) {
					continue;
				}
				auto *e = GetOrQueryProviderEntry(n);
				if (e) {
					callback(*e);
					seen.insert(n);
				}
			}
		}
	}

	lock_guard<mutex> lock(bridge_lock);
	if (!bridge_info) {
		return;
	}
	vector<string> bridge_names;
	{
		lock_guard<mutex> bl(bridge_info->mtx);
		for (auto &kv : bridge_info->permissions) {
			bridge_names.push_back(kv.first);
		}
	}
	for (auto &n : bridge_names) {
		if (seen.count(n)) {
			continue;
		}
		auto *e = GetOrFillBridgeEntry(context, n);
		if (e) {
			callback(*e);
			seen.insert(n);
		}
	}
}

void VirtualCatalogSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// No ClientContext here; bridge entries need one to resolve source schemas, so skip them.
	target_schema.Scan(type, callback);

	if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
		return;
	}
	auto seen = CollectNativeNames(target_schema, nullptr, type);

	lock_guard<mutex> lock(provider_lock);
	if (!provider) {
		return;
	}
	for (auto &n : GetOrQueryProviderTableNames()) {
		if (seen.count(n)) {
			continue;
		}
		auto *e = GetOrQueryProviderEntry(n);
		if (e) {
			callback(*e);
		}
	}
}

void VirtualCatalogSchemaEntry::CollectPermissions(ClientContext &context, optional_ptr<const string> table_filter,
                                                   vector<TablePermissionRow> &out) {
	const auto matches = [&](const string &n) {
		return !table_filter || StringUtil::CIEquals(n, *table_filter);
	};
	case_insensitive_set_t seen;

	CollectNativeSchemaPermissions(context, target_schema, name, table_filter, seen, out);

	// Provider capability: writeable == insert_udf present, editable == alter_udf present.
	{
		lock_guard<mutex> lock(provider_lock);
		if (provider) {
			bool writeable = !provider->insert_udf.empty();
			bool editable = !provider->alter_udf.empty();
			for (auto &n : GetOrQueryProviderTableNames()) {
				if (!matches(n) || seen.count(n)) {
					continue;
				}
				seen.insert(n);
				vector<string> pk;
				auto *e = GetOrQueryProviderEntry(n);
				if (e) {
					pk = e->Cast<ProviderTableCatalogEntry>().table_info->primary_keys;
				}
				out.push_back({name, n, "provider", writeable, editable, std::move(pk)});
			}
		}
	}

	lock_guard<mutex> lock(bridge_lock);
	if (!bridge_info) {
		return;
	}
	lock_guard<mutex> bl(bridge_info->mtx);
	for (auto &kv : bridge_info->permissions) {
		if (!matches(kv.first) || seen.count(kv.first)) {
			continue;
		}
		seen.insert(kv.first);
		bool writeable = kv.second == TablePermission::READWRITE;
		vector<string> pk;
		auto pk_it = bridge_info->primary_keys.find(kv.first);
		if (pk_it != bridge_info->primary_keys.end()) {
			pk = pk_it->second;
		}
		out.push_back(
		    {name, kv.first, writeable ? "bridge_readwrite" : "bridge_read", writeable, false, std::move(pk)});
	}
}

} // namespace duckdb
