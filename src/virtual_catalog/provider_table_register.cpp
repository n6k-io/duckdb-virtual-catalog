#include "vcat_catalog.hpp"
#include "vcat_schema_entry.hpp"
#include "shared_registry.hpp"
#include "provider_info.hpp"
#include "provider_table_catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// Process-wide ProviderInfo registry; the catalog entries themselves live on the
// VirtualCatalogSchemaEntry.
static vcat::SharedRegistry<ProviderInfo> &ProviderRegistry() {
	static vcat::SharedRegistry<ProviderInfo> instance;
	return instance;
}

static string ProviderKey(const string &catalog, const string &schema) {
	return catalog + "::" + schema;
}

shared_ptr<ProviderInfo> GetProvider(const string &catalog, const string &schema) {
	return ProviderRegistry().Get(ProviderKey(catalog, schema));
}

// Put, not Insert: re-registering the same (catalog, schema) is how a host swaps its UDF set, so a
// duplicate replaces rather than fails. The bridge registry claims with Insert instead, because
// there a duplicate id means two callers wanted the same name.
void RegisterProvider(const shared_ptr<ProviderInfo> &info) {
	ProviderRegistry().Put(ProviderKey(info->catalog_name, info->schema_name), info);
}

static bool UnregisterProvider(const string &catalog, const string &schema) {
	return ProviderRegistry().Erase(ProviderKey(catalog, schema));
}

bool BumpProviderVersion(ClientContext & /*context*/, const string &catalog, const string &schema) {
	auto info = GetProvider(catalog, schema);
	if (!info) {
		return false;
	}
	info->version.fetch_add(1, std::memory_order_release);
	return true;
}

static void AttachProviderToSchemaAndRegister(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].Flatten(count);
	}

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto schemas = FlatVector::GetData<string_t>(args.data[1]);
	auto probe_ids = FlatVector::GetData<string_t>(args.data[2]);
	auto list_udfs = FlatVector::GetData<string_t>(args.data[3]);
	auto schema_udfs = FlatVector::GetData<string_t>(args.data[4]);
	auto scan_udfs = FlatVector::GetData<string_t>(args.data[5]);
	auto insert_udfs = FlatVector::GetData<string_t>(args.data[6]);
	auto update_udfs = FlatVector::GetData<string_t>(args.data[7]);
	auto delete_udfs = FlatVector::GetData<string_t>(args.data[8]);
	auto alter_udfs = FlatVector::GetData<string_t>(args.data[9]);

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto target_catalog_name = catalogs[i].GetString();
		auto target_schema_name = schemas[i].GetString();

		auto catalog_entry = Catalog::GetCatalogEntry(context, target_catalog_name);
		if (!catalog_entry) {
			throw CatalogException("vcat_provider: target catalog '%s' does not exist", target_catalog_name);
		}
		auto *vcat = dynamic_cast<VirtualCatalog *>(catalog_entry.get());
		if (!vcat) {
			throw CatalogException(
			    "vcat_provider: catalog '%s' is not TYPE virtual_catalog; ATTACH with (TYPE virtual_catalog) first",
			    target_catalog_name);
		}

		EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, target_schema_name);
		auto &db_instance = catalog_entry->GetAttached().GetDatabase();
		auto sys_transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto schema_entry = vcat->LookupSchema(sys_transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
		if (!schema_entry) {
			throw CatalogException("vcat_provider: schema '%s.%s' does not exist; CREATE SCHEMA first",
			                       target_catalog_name, target_schema_name);
		}
		auto *local_schema = dynamic_cast<VirtualCatalogSchemaEntry *>(schema_entry.get());
		if (!local_schema) {
			throw CatalogException("vcat_provider: schema '%s.%s' is not a provider-capable schema",
			                       target_catalog_name, target_schema_name);
		}

		auto info = make_shared_ptr<ProviderInfo>();
		info->probe_id = probe_ids[i].GetString();
		info->catalog_name = target_catalog_name;
		info->schema_name = target_schema_name;
		info->db_instance = context.db;
		info->list_udf = list_udfs[i].GetString();
		info->schema_udf = schema_udfs[i].GetString();
		info->scan_udf = scan_udfs[i].GetString();
		info->insert_udf = insert_udfs[i].GetString();
		info->update_udf = update_udfs[i].GetString();
		info->delete_udf = delete_udfs[i].GetString();
		info->alter_udf = alter_udfs[i].GetString();
		info->phantom_catalog = make_shared_ptr<ProviderTableCatalog>(catalog_entry->GetAttached());

		local_schema->SetProvider(info);
		RegisterProvider(info);

		result_data[i] = StringVector::AddString(result, "ok");
	}
}

static void DetachProviderFromSchemaAndUnregister(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	args.data[1].Flatten(count);

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto schemas = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto target_catalog_name = catalogs[i].GetString();
		auto target_schema_name = schemas[i].GetString();

		auto catalog_entry = Catalog::GetCatalogEntry(context, target_catalog_name);
		if (!catalog_entry) {
			throw CatalogException("vcat_provider: target catalog '%s' does not exist", target_catalog_name);
		}
		auto *vcat = dynamic_cast<VirtualCatalog *>(catalog_entry.get());
		if (!vcat) {
			throw CatalogException(
			    "vcat_provider: catalog '%s' is not TYPE virtual_catalog; ATTACH with (TYPE virtual_catalog) first",
			    target_catalog_name);
		}

		EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, target_schema_name);
		auto &db_instance = catalog_entry->GetAttached().GetDatabase();
		auto sys_transaction = CatalogTransaction::GetSystemTransaction(db_instance);
		auto schema_entry = vcat->LookupSchema(sys_transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
		auto *local_schema = schema_entry ? dynamic_cast<VirtualCatalogSchemaEntry *>(schema_entry.get()) : nullptr;

		// Detach from the schema before dropping the registry entry so racing plan compilation stops routing here.
		if (local_schema) {
			local_schema->SetProvider(nullptr);
		}
		if (!UnregisterProvider(target_catalog_name, target_schema_name)) {
			throw CatalogException("vcat_provider: no provider registered for %s.%s", target_catalog_name,
			                       target_schema_name);
		}
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

static void BumpProviderVersionOrThrow(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	args.data[1].Flatten(count);

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto schemas = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto catalog = catalogs[i].GetString();
		auto schema = schemas[i].GetString();
		if (!BumpProviderVersion(context, catalog, schema)) {
			throw CatalogException("vcat_provider: no provider registered for %s.%s", catalog, schema);
		}
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void RegisterProviderFunctions(ExtensionLoader &loader) {
	ScalarFunction register_func(
	    "vcat_register_provider",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, AttachProviderToSchemaAndRegister);
	loader.RegisterFunction(register_func);

	ScalarFunction invalidate_func("vcat_invalidate_provider_tables", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, BumpProviderVersionOrThrow);
	loader.RegisterFunction(invalidate_func);

	ScalarFunction unregister_func("vcat_unregister_provider", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, DetachProviderFromSchemaAndUnregister);
	loader.RegisterFunction(unregister_func);
}

} // namespace duckdb
