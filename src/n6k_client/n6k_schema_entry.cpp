#include "n6k_schema_entry.hpp"
#include "n6k_yyjson_util.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_table_entry.hpp"
#include "n6k_rpc_function.hpp"
#include "n6k_fetch.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "nanoarrow.h"
#include "yyjson.hpp"

namespace duckdb {

namespace {

using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_append;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_write;

} // namespace

N6kSchemaEntry::N6kSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, string base_url_p,
                               std::shared_ptr<CatalogSession> session_p)
    : SchemaCatalogEntry(catalog, info), base_url(std::move(base_url_p)), session(std::move(session_p)),
      fetched_tables(false) {
}

N6kCatalog &N6kSchemaEntry::GetN6kCatalog() {
	return catalog.Cast<N6kCatalog>();
}

optional_ptr<CatalogEntry> N6kSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	auto entry_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();

	if (entry_type == CatalogType::TABLE_ENTRY || entry_type == CatalogType::VIEW_ENTRY) {
		return LookupTableEntry(transaction, entry_name);
	}
	if (entry_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		return GetOrCreateTableFunctionEntry(transaction, entry_name);
	}
	return nullptr;
}

optional_ptr<CatalogEntry> N6kSchemaEntry::LookupTableEntry(CatalogTransaction transaction, const string &entry_name) {
	// consume invalidations before cache-hit so no stale entry served; needs context to refetch
	if (transaction.HasContext()) {
		EnsureFresh(transaction.GetContext());
	}

	lock_guard<mutex> lock(tables_lock);
	auto it = tables.find(entry_name);
	if (it != tables.end()) {
		return it->second.get();
	}
	return nullptr;
}

optional_ptr<CatalogEntry> N6kSchemaEntry::GetOrCreateTableFunctionEntry(CatalogTransaction transaction,
                                                                         const string &entry_name) {
	{
		lock_guard<mutex> lock(table_functions_lock);
		auto it = table_functions.find(entry_name);
		if (it != table_functions.end()) {
			return it->second.get();
		}
	}
	TableFunction tf;
	if (entry_name == "exec") {
		tf = MakeN6kCatalogExecFunction(base_url, session);
	} else if (entry_name == "query") {
		tf = MakeN6kCatalogQueryFunction(base_url, session);
	} else {
		tf = MakeN6kCatalogRpcFunction(base_url, entry_name, session);
	}
	auto info = CreateTableFunctionInfo(tf);
	auto entry = make_uniq<TableFunctionCatalogEntry>(catalog, *this, info);
	auto *ptr = entry.get();
	lock_guard<mutex> lock(table_functions_lock);
	table_functions[entry_name] = std::move(entry);
	return ptr;
}

void N6kSchemaEntry::RetireTableEntry(const string &entry_name) {
	auto it = tables.find(entry_name);
	if (it == tables.end()) {
		return;
	}
	retired_tables.push_back(std::move(it->second));
	tables.erase(it);
}

void N6kSchemaEntry::RetireAllTableEntries() {
	for (auto &kv : tables) {
		retired_tables.push_back(std::move(kv.second));
	}
	tables.clear();
}

void N6kSchemaEntry::LoadMissingTableEntries(ClientContext &context) {
	auto table_rows = session->ListTables();

	auto &n6k_catalog = GetN6kCatalog();
	auto system_transaction = CatalogTransaction::GetSystemTransaction(catalog.GetDatabase());

	for (auto &row : table_rows) {
		auto &schema_name = row.schema;
		auto &table_name = row.name;

		if (schema_name != name) {
			continue;
		}

		n6k_catalog.GetOrCreateSchema(system_transaction, schema_name);

		{
			lock_guard<mutex> lock(tables_lock);
			if (tables.find(table_name) != tables.end()) {
				continue;
			}
		}

		ArrowArrayStream schema_stream;
		schema_stream.release = nullptr;
		session->FetchTableSchema(schema_name, table_name, &schema_stream);

		ArrowSchema arrow_schema;
		if (schema_stream.get_schema(&schema_stream, &arrow_schema) != 0) {
			if (schema_stream.release) {
				schema_stream.release(&schema_stream);
			}
			// Skipping would drop this table from the catalog, and `fetched_tables` latches below, so it
			// would stay gone until an invalidation -- indistinguishable from a table the server never
			// listed. FetchTableSchema already threw on a server-side error, so reaching here means the
			// Arrow payload itself is malformed.
			throw IOException("n6k[%s]: table \"%s.%s\" returned an unreadable Arrow schema", catalog.GetName(),
			                  schema_name, table_name);
		}

		auto create_info = make_uniq<CreateTableInfo>(*this, table_name);
		for (int i = 0; i < arrow_schema.n_children; i++) {
			auto *child = arrow_schema.children[i];
			string col_name = child->name;
			auto arrow_type = ArrowType::GetArrowLogicalType(context, *child);
			create_info->columns.AddColumn(ColumnDefinition(col_name, arrow_type->GetDuckType(true)));
		}

		if (arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
		if (schema_stream.release) {
			schema_stream.release(&schema_stream);
		}

		auto table_entry = make_uniq<N6kTableCatalogEntry>(catalog, *this, *create_info, base_url, row.schema, row.name,
		                                                   row.writable, row.editable, row.primary_keys, session);
		lock_guard<mutex> lock(tables_lock);
		RetireTableEntry(table_name);
		tables[table_name] = std::move(table_entry);
	}

	lock_guard<mutex> lock(tables_lock);
	fetched_tables = true;
}

void N6kSchemaEntry::EnsureFresh(ClientContext &context) {
	// pull pending server PUSH events first (wasm drains here; native's WS-reader already delivered)
	session->PollPushEvents();

	// One loader at a time. Without this, two scans in the same query both observe fetched_tables == false,
	// both miss the same table in LoadMissingTableEntries' pre-check, and the second insert frees the entry
	// the first already published to a running scan.
	lock_guard<mutex> load(load_lock);

	// apply invalidation here (tables_lock only, never held across I/O); retire cache since
	// LoadMissingTableEntries only inserts
	if (stale_.exchange(false, std::memory_order_acq_rel)) {
		lock_guard<mutex> lock(tables_lock);
		RetireAllTableEntries();
		fetched_tables = false;
	}

	bool needs_load;
	{
		lock_guard<mutex> lock(tables_lock);
		needs_load = !fetched_tables;
	}
	if (needs_load) {
		LoadMissingTableEntries(context);
	}
}

void N6kSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		EnsureFresh(context);
		lock_guard<mutex> lock(tables_lock);
		for (auto &kv : tables) {
			callback(*kv.second);
		}
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		lock_guard<mutex> lock(table_functions_lock);
		for (auto &kv : table_functions) {
			callback(*kv.second);
		}
	}
}

void N6kSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		lock_guard<mutex> lock(tables_lock);
		for (auto &kv : tables) {
			callback(*kv.second);
		}
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		lock_guard<mutex> lock(table_functions_lock);
		for (auto &kv : table_functions) {
			callback(*kv.second);
		}
	}
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &create_info = info.Base();

	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, arr);
	for (auto &col : create_info.columns.Logical()) {
		auto *col_obj = yyjson_mut_obj(doc);
		const auto &name = col.Name();
		auto type_str = col.Type().ToString();
		yyjson_mut_obj_add_strncpy(doc, col_obj, "name", name.c_str(), name.size());
		yyjson_mut_obj_add_strncpy(doc, col_obj, "type", type_str.c_str(), type_str.size());
		yyjson_mut_arr_append(arr, col_obj);
	}
	auto columns_json = n6k::SerializeJsonDocAndFree(doc);

	session->CreateTable(name, create_info.table, columns_json);

	auto table_entry = make_uniq<N6kTableCatalogEntry>(catalog, *this, create_info, base_url, name, create_info.table,
	                                                   /*writable=*/true, /*editable=*/true,
	                                                   /*primary_key=*/vector<string> {}, session);
	auto result = table_entry.get();
	lock_guard<mutex> lock(tables_lock);
	RetireTableEntry(create_info.table);
	tables[create_info.table] = std::move(table_entry);
	return result;
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Creating functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (!transaction.HasContext()) {
		throw BinderException("Cannot create view without client context");
	}
	auto &context = transaction.GetContext();

	// forward raw SQL: preserves original catalog/schema qualifiers
	auto sql = context.GetCurrentQuery();
	session->Exec(sql);

	lock_guard<mutex> load(load_lock);
	{
		lock_guard<mutex> lock(tables_lock);
		fetched_tables = false;
		RetireTableEntry(info.view_name);
	}

	LoadMissingTableEntries(context);

	lock_guard<mutex> lock(tables_lock);
	auto it = tables.find(info.view_name);
	if (it != tables.end()) {
		return it->second.get();
	}
	return nullptr;
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw BinderException("Creating indexes in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Creating sequences in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw BinderException("Creating table functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw BinderException("Creating copy functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw BinderException("Creating pragma functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Creating collations in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Creating types in n6k attached databases is not supported");
}

void N6kSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::VIEW_ENTRY) {
		auto sql = context.GetCurrentQuery();
		session->Exec(sql);

		lock_guard<mutex> lock(tables_lock);
		RetireTableEntry(info.name);
		fetched_tables = false;
		return;
	}
	throw BinderException("Dropping entries in n6k attached databases is not supported");
}

void N6kSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only ALTER TABLE is supported on n6k attached databases");
	}
	auto &alter = info.Cast<AlterTableInfo>();

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
		throw BinderException(
		    "ALTER TABLE %s is not supported on n6k attached databases (only ADD COLUMN, DROP COLUMN, RENAME COLUMN)",
		    alter.ToString());
	}
	auto details_json = n6k::SerializeJsonDocAndFree(doc);

	session->AlterTable(alter.schema, alter.name, kind, details_json);

	lock_guard<mutex> lock(tables_lock);
	fetched_tables = false;
	RetireTableEntry(alter.name);
}

void N6kSchemaEntry::Invalidate() {
	stale_.store(true, std::memory_order_release);
}

} // namespace duckdb
