#include "n6k_table_entry.hpp"
#include "n6k_arrow_stream.hpp"
#include "n6k_catalog_session.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

N6kTableCatalogEntry::N6kTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           string base_url_p, string table_schema_p, string table_name_p,
                                           bool writable_p, bool editable_p, vector<string> primary_key_p,
                                           std::shared_ptr<CatalogSession> session_p)
    : TableCatalogEntry(catalog, schema, info), base_url(std::move(base_url_p)),
      table_schema(std::move(table_schema_p)), table_name(std::move(table_name_p)), writable(writable_p),
      editable(editable_p), primary_key(std::move(primary_key_p)), session(std::move(session_p)) {
}

unique_ptr<BaseStatistics> N6kTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

static BindInfo N6kScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<N6kLazyScanFunctionData>();
	return BindInfo(*data.table);
}

TableFunction N6kTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	if (!session) {
		throw IOException("n6k: attached-catalog scans require a WebSocket session; base_url=" + base_url);
	}

	auto schema_stream = make_uniq<N6kStreamData>();
	session->FetchTableSchema(table_schema, table_name, &schema_stream->stream);

	auto lazy_stream = make_uniq<N6kLazyStreamData>(context, session, table_schema, table_name);

	auto result = make_uniq<N6kLazyScanFunctionData>(reinterpret_cast<stream_factory_produce_t>(N6kProduceLazyStream),
	                                                 std::move(lazy_stream), std::move(schema_stream));
	result->projection_pushdown_enabled = true;

	auto *schema_ptr = result->owned_schema_stream.get();
	schema_ptr->stream.get_schema(&schema_ptr->stream, &result->schema_root.arrow_schema);
	result->owned_stream_data->schema = &result->schema_root.arrow_schema;

	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	result->all_types = result->arrow_table.GetTypes();
	result->table = this;

	bind_data = std::move(result);

	TableFunction scan_func("n6k_scan", {}, ArrowTableFunction::ArrowScanFunction, nullptr,
	                        ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	scan_func.projection_pushdown = true;
	scan_func.filter_pushdown = true;
	scan_func.get_bind_info = N6kScanGetBindInfo;
	return scan_func;
}

TableStorageInfo N6kTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

} // namespace duckdb
