#include "bridge_table_entry.hpp"
#include "bridge_table_function.hpp"
#include "bridge_scan_shared.hpp"
#include "bridge_pushdown.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

BridgeTableCatalogEntry::BridgeTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                                 shared_ptr<BridgeInfo> bridge_info_p, string source_table_name_p)
    : TableCatalogEntry(catalog, schema, info), bridge_info(std::move(bridge_info_p)),
      source_table_name(std::move(source_table_name_p)) {
}

unique_ptr<BaseStatistics> BridgeTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

static unique_ptr<ArrowArrayStreamWrapper> RunSourceQueryWithPKColumnsAndStream(uintptr_t factory_ptr,
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
	if (!stream_data->include_pk_columns.empty()) {
		auto from_pos = sql.find(" FROM ");
		if (from_pos != string::npos) {
			string pk_cols;
			for (auto &col : stream_data->include_pk_columns) {
				pk_cols += ", " + KeywordHelper::WriteOptionallyQuoted(col);
			}
			sql.insert(from_pos, pk_cols);
		}
	}

	stream_data->source_conn = make_uniq<Connection>(*stream_data->bridge_info->source_db);
	auto query_result = stream_data->source_conn->SendQuery(sql);

	if (query_result->HasError()) {
		query_result->GetErrorObject().Throw("virtual_catalog: query on source failed: ");
	}

	if (!stream_data->include_pk_columns.empty()) {
		auto &result_types = query_result->types;
		idx_t pk_start = result_types.size() - stream_data->include_pk_columns.size();
		for (idx_t i = pk_start; i < result_types.size(); i++) {
			stream_data->pk_types.push_back(result_types[i]);
		}
	}

	auto *arrow_stream_owner = new ResultArrowArrayStreamWrapper(std::move(query_result), 2048);

	wrapper->arrow_array_stream = arrow_stream_owner->stream;
	arrow_stream_owner->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

struct BridgeScanGlobalState : public ArrowScanGlobalState {
	idx_t pk_start_index = COLUMN_IDENTIFIER_ROW_ID; // sentinel = no PK columns
	idx_t pk_count = 0;
	shared_ptr<BridgePKBuffer> pk_buffer;
	vector<LogicalType> pk_types;
	vector<string> pk_formats;
};

static unique_ptr<GlobalTableFunctionState> BridgeScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BridgeScanFunctionData>();
	auto result = make_uniq<BridgeScanGlobalState>();

	idx_t projected_count = 0;
	for (auto &col_id : input.column_ids) {
		if (col_id != COLUMN_IDENTIFIER_ROW_ID) {
			projected_count++;
		}
	}

	if (bind_data.owned_stream_data && !bind_data.owned_stream_data->include_pk_columns.empty()) {
		result->pk_start_index = projected_count;
		result->pk_count = bind_data.owned_stream_data->include_pk_columns.size();
		result->pk_buffer = bind_data.owned_stream_data->pk_buffer;
	}

	result->stream = BuildProjectionParamsAndProduceStream(bind_data, input.column_ids, input.filters.get());

	// Map stream column indices to original table column indices so ArrowToDuckDB looks up post-pushdown.
	if (result->stream && result->stream->arrow_array_stream.get_schema) {
		ArrowSchema stream_schema;
		result->stream->arrow_array_stream.get_schema(&result->stream->arrow_array_stream, &stream_schema);
		idx_t stream_col = 0;
		for (idx_t idx = 0; idx < input.column_ids.size(); idx++) {
			auto col_id = input.column_ids[idx];
			if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			if (stream_col < static_cast<idx_t>(stream_schema.n_children)) {
				auto &schema = *stream_schema.children[stream_col];
				auto arrow_type = ArrowType::GetArrowLogicalType(context, schema);
				bind_data.arrow_table.AddColumn(col_id, std::move(arrow_type), string(schema.name));
				stream_col++;
			}
		}
		if (stream_schema.release) {
			stream_schema.release(&stream_schema);
		}
	}

	// pk_types are populated during stream production above, so copy them after.
	if (bind_data.owned_stream_data && !bind_data.owned_stream_data->pk_types.empty()) {
		result->pk_types = bind_data.owned_stream_data->pk_types;
	}
	result->max_threads = context.db->NumberOfThreads();
	if (!input.projection_ids.empty()) {
		result->projection_ids = input.projection_ids;
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(bind_data.all_types[col_idx]);
			}
		}
	}
	return std::move(result);
}

static void BridgeScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
	auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
	auto &global_state = data_p.global_state->Cast<BridgeScanGlobalState>();

	if (state.chunk_offset >= static_cast<idx_t>(state.chunk->arrow_array.length)) {
		if (!ArrowTableFunction::ArrowScanParallelStateNext(context, data_p.bind_data.get(), state, global_state)) {
			return;
		}
	}
	auto output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset);
	data.lines_read += output_size;

	bool has_pk_columns = global_state.pk_start_index != COLUMN_IDENTIFIER_ROW_ID && global_state.pk_buffer;

	if (global_state.CanRemoveFilterColumns()) {
		state.all_columns.Reset();
		state.all_columns.SetCardinality(output_size);
		ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), state.all_columns);
		if (has_pk_columns) {
			for (idx_t idx = 0; idx < state.all_columns.ColumnCount(); idx++) {
				auto col_id = state.column_ids.empty() ? idx : state.column_ids[idx];
				if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
					AppendPKValuesAndEmitBufferRowIds(state.all_columns.data[idx], state.chunk->arrow_array,
					                                  global_state, state.chunk_offset, output_size, "virtual_catalog");
				}
			}
		}
		output.ReferenceColumns(state.all_columns, global_state.projection_ids);
	} else {
		output.SetCardinality(output_size);
		ArrowTableFunction::ArrowToDuckDB(state, data.arrow_table.GetColumns(), output);
		if (has_pk_columns) {
			for (idx_t idx = 0; idx < output.ColumnCount(); idx++) {
				auto col_id = state.column_ids.empty() ? idx : state.column_ids[idx];
				if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
					AppendPKValuesAndEmitBufferRowIds(output.data[idx], state.chunk->arrow_array, global_state,
					                                  state.chunk_offset, output_size, "virtual_catalog");
				}
			}
		}
	}

	output.Verify();
	state.chunk_offset += output.size();
}

static BindInfo BridgeScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<BridgeScanFunctionData>();
	return BindInfo(*data.table);
}

TableFunction BridgeTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasReadPermission(source_table_name)) {
			throw PermissionException("virtual_catalog: '%s' does not have read permission in bridge '%s'",
			                          source_table_name, bridge_info->bridge_id);
		}
	}

	auto stream_data = make_uniq<BridgeStreamData>();
	stream_data->bridge_info = bridge_info;
	stream_data->table_name = bridge_info->source_catalog + "." + bridge_info->source_schema + "." + source_table_name;
	stream_data->consumed = false;

	for (auto &col : columns.Logical()) {
		stream_data->column_names.push_back(col.GetName());
	}

	auto result = make_uniq<BridgeScanFunctionData>(
	    reinterpret_cast<stream_factory_produce_t>(RunSourceQueryWithPKColumnsAndStream), std::move(stream_data));

	result->projection_pushdown_enabled = true;

	// arrow_table is populated later in BridgeScanInitGlobal from the pushdown-reflecting stream schema.
	for (auto &col : columns.Logical()) {
		result->all_types.push_back(col.GetType());
	}

	result->table = this;
	bind_data = std::move(result);

	TableFunction scan_func("virtual_catalog_table_scan", {}, BridgeScanFunction, nullptr, BridgeScanInitGlobal,
	                        ArrowTableFunction::ArrowScanInitLocal);
	scan_func.projection_pushdown = true;
	scan_func.filter_pushdown = true;
	scan_func.get_bind_info = BridgeScanGetBindInfo;
	return scan_func;
}

TableStorageInfo BridgeTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

} // namespace duckdb
