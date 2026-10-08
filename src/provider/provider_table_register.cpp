#include "provider_virtual_catalog.hpp"
#include "provider_info.hpp"
#include "provider_table_catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

static VirtualCatalogProvider &LookupProviderCatalogOrThrow(ClientContext &context, const string &catalog_name) {
	auto catalog_entry = Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog_entry) {
		throw CatalogException("virtual_catalog_provider: target catalog '%s' does not exist", catalog_name);
	}
	auto *vcat = dynamic_cast<VirtualCatalogProvider *>(catalog_entry.get());
	if (!vcat) {
		throw CatalogException("virtual_catalog_provider: catalog '%s' is not TYPE virtual_catalog_provider; ATTACH "
		                       "with (TYPE virtual_catalog_provider) first",
		                       catalog_name);
	}
	return *vcat;
}

static void AttachProviderToCatalogAndRegister(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < args.ColumnCount(); c++) {
		args.data[c].Flatten(count);
	}

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto list_udfs = FlatVector::GetData<string_t>(args.data[1]);
	auto schema_udfs = FlatVector::GetData<string_t>(args.data[2]);
	auto scan_udfs = FlatVector::GetData<string_t>(args.data[3]);
	auto insert_udfs = FlatVector::GetData<string_t>(args.data[4]);
	auto update_udfs = FlatVector::GetData<string_t>(args.data[5]);
	auto delete_udfs = FlatVector::GetData<string_t>(args.data[6]);
	auto alter_udfs = FlatVector::GetData<string_t>(args.data[7]);

	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto target_catalog_name = catalogs[i].GetString();
		auto &vcat = LookupProviderCatalogOrThrow(context, target_catalog_name);

		auto info = make_shared_ptr<ProviderInfo>();
		info->catalog_name = target_catalog_name;
		info->db_instance = context.db;
		info->list_udf = list_udfs[i].GetString();
		info->schema_udf = schema_udfs[i].GetString();
		info->scan_udf = scan_udfs[i].GetString();
		info->insert_udf = insert_udfs[i].GetString();
		info->update_udf = update_udfs[i].GetString();
		info->delete_udf = delete_udfs[i].GetString();
		info->alter_udf = alter_udfs[i].GetString();
		if (info->list_udf.empty() || info->schema_udf.empty() || info->scan_udf.empty()) {
			throw BinderException("virtual_catalog_provider: provider_register needs non-empty list, schema and scan "
			                      "function names");
		}
		info->phantom_catalog = make_shared_ptr<ProviderTableCatalog>(vcat.GetAttached());

		vcat.SetProvider(info);

		result_data[i] = StringVector::AddString(result, "ok");
	}
}

static void DetachProviderFromCatalogAndUnregister(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto target_catalog_name = catalogs[i].GetString();
		auto &vcat = LookupProviderCatalogOrThrow(context, target_catalog_name);
		if (!vcat.GetProvider()) {
			throw CatalogException("virtual_catalog_provider: no provider registered for %s", target_catalog_name);
		}
		vcat.SetProvider(nullptr);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

static void BumpProviderVersionOrThrow(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);

	auto catalogs = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto catalog = catalogs[i].GetString();
		auto info = LookupProviderCatalogOrThrow(context, catalog).GetProvider();
		if (!info) {
			throw CatalogException("virtual_catalog_provider: no provider registered for %s", catalog);
		}
		info->version.fetch_add(1, std::memory_order_release);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void RegisterProviderFunctions(ExtensionLoader &loader) {
	ScalarFunction register_func("provider_register",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                              LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                              LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             LogicalType::VARCHAR, AttachProviderToCatalogAndRegister);
	register_func.SetVolatile();
	loader.RegisterFunction(register_func);

	ScalarFunction invalidate_func("provider_invalidate_tables", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               BumpProviderVersionOrThrow);
	invalidate_func.SetVolatile();
	loader.RegisterFunction(invalidate_func);

	ScalarFunction unregister_func("provider_unregister", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               DetachProviderFromCatalogAndUnregister);
	unregister_func.SetVolatile();
	loader.RegisterFunction(unregister_func);
}

} // namespace duckdb
