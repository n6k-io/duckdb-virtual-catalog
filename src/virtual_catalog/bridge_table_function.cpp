#include "bridge_table_function.hpp"
#include "bridge_pushdown.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static unique_ptr<ArrowArrayStreamWrapper> BridgeProduceStream(uintptr_t factory_ptr,
                                                               ArrowStreamParameters &parameters) {
	auto *stream_data = reinterpret_cast<BridgeStreamData *>(factory_ptr);

	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}

	if (!stream_data->bridge_info->source_db) {
		throw IOException("virtual_catalog: source database for bridge '%s' is unavailable",
		                  stream_data->bridge_info->bridge_id);
	}

	auto sql = BuildBridgeSQL(stream_data->table_name, parameters, stream_data->column_names);
	DUCKDB_LOG_DEBUG(*stream_data->bridge_info->source_db, "virtual_catalog: pushdown sql: %s", sql);

	stream_data->source_conn = make_uniq<Connection>(*stream_data->bridge_info->source_db);
	auto query_result = stream_data->source_conn->SendQuery(sql);

	if (query_result->HasError()) {
		query_result->GetErrorObject().Throw("virtual_catalog: query on source failed: ");
	}

	// Owned via its own MyStreamRelease callback (delete on private_data) — do NOT also hold in unique_ptr.
	auto *arrow_stream_owner = new ResultArrowArrayStreamWrapper(std::move(query_result), 2048);

	wrapper->arrow_array_stream = arrow_stream_owner->stream;
	arrow_stream_owner->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

static unique_ptr<FunctionData> BridgeBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto bridge_id = input.inputs[0].GetValue<string>();
	auto table_name = input.inputs[1].GetValue<string>();
	DUCKDB_LOG_DEBUG(context, "virtual_catalog: bind bridge='%s' table='%s'", bridge_id, table_name);

	auto bridge_info = GetBridge(bridge_id);
	if (!bridge_info) {
		throw IOException("virtual_catalog: bridge '%s' not found", bridge_id);
	}

	if (!bridge_info->source_db) {
		throw IOException("virtual_catalog: source database for bridge '%s' is unavailable", bridge_id);
	}

	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasReadPermission(table_name)) {
			throw PermissionException("virtual_catalog: '%s' does not have read permission in bridge '%s'", table_name,
			                          bridge_id);
		}
	}

	// schema_wrapper + schema_conn are retained to keep the Arrow schema memory alive for arrow_table.
	auto source_conn = make_uniq<Connection>(*bridge_info->source_db);
	auto qualified_table = bridge_info->QualifiedSourceTable(table_name);
	auto schema_result = source_conn->SendQuery("SELECT * FROM " + qualified_table + " LIMIT 0");

	if (schema_result->HasError()) {
		throw IOException("virtual_catalog: failed to get schema for '%s' on source: %s", table_name,
		                  schema_result->GetError());
	}

	auto stream_data = make_uniq<BridgeStreamData>();
	stream_data->bridge_info = bridge_info;
	stream_data->table_name = bridge_info->source_catalog + "." + bridge_info->source_schema + "." + table_name;
	stream_data->consumed = false;

	auto result = make_uniq<BridgeScanFunctionData>(reinterpret_cast<stream_factory_produce_t>(BridgeProduceStream),
	                                                std::move(stream_data));

	result->projection_pushdown_enabled = true;
	result->owned_schema_wrapper = make_uniq<ResultArrowArrayStreamWrapper>(std::move(schema_result), 1);
	result->owned_schema_wrapper->stream.get_schema(&result->owned_schema_wrapper->stream,
	                                                &result->schema_root.arrow_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	result->schema_conn = std::move(source_conn);

	for (int64_t i = 0; i < result->schema_root.arrow_schema.n_children; i++) {
		result->owned_stream_data->column_names.emplace_back(result->schema_root.arrow_schema.children[i]->name);
	}

	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;

	DUCKDB_LOG_DEBUG(context, "virtual_catalog: bind complete, %d columns", names.size());
	return std::move(result);
}

void RegisterBridgeTableFunction(ExtensionLoader &loader) {
	TableFunction bridge_scan("vcat_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          ArrowTableFunction::ArrowScanFunction, BridgeBind,
	                          ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	bridge_scan.projection_pushdown = true;
	bridge_scan.filter_pushdown = true;
	loader.RegisterFunction(bridge_scan);
}

} // namespace duckdb
