#include "provider_table_entry.hpp"
#include "bridge_table_function.hpp"
#include "bridge_scan_shared.hpp"
#include "filter_json.hpp"
#include "provider_arrow.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/common/constants.hpp"

namespace duckdb {

ProviderTableCatalogEntry::ProviderTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                     CreateTableInfo &info, shared_ptr<ProviderTableInfo> info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(info_p)) {
}

unique_ptr<BaseStatistics> ProviderTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

static string SerializeFlatFiltersOrThrow(ArrowStreamParameters &parameters, const vector<string> &column_names) {
	if (!parameters.filters) {
		return "";
	}
	// The provider contract is the flat pyarrow tuple shape, which cannot express a disjunction -- so
	// OR filters are refused here rather than silently flattened -- and which tags the literals that
	// crossed as text, without which the callee has to ask the provider for its schema again on every
	// filtered scan just to learn that '2026-05-01' was a DATE.
	filter_json::FilterSerializeResult result;
	auto json = filter_json::SerializeFilters(*parameters.filters, parameters.projected_columns.filter_to_col,
	                                              column_names, result);
	if (!result.all_exact) {
		filter_json::ThrowUnrenderableFilter("vcat_provider scan", result.first_unsupported);
	}
	return json;
}

static unique_ptr<ArrowArrayStreamWrapper> RunScanUdfAndDecodeArrowResult(uintptr_t factory_ptr,
                                                                          ArrowStreamParameters &parameters) {
	auto *stream_data = reinterpret_cast<BridgeStreamData *>(factory_ptr);

	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}

	if (!stream_data->bridge_info || !stream_data->bridge_info->source_db) {
		throw IOException("vcat_provider: database instance unavailable");
	}

	auto &projected = parameters.projected_columns.columns;
	auto &pk_cols = stream_data->include_pk_columns;

	// The exact column layout the scan state expects: the projection, then the primary key columns
	// trailing it, so pk_start_index == projected.size(). A key that is also projected is asked for
	// TWICE on purpose — the row-id machinery reads the trailing block positionally, so the duplicate
	// has to be there. The Python side dedups before it calls the provider, which never sees this.
	vector<Value> output_cols;
	for (auto &col : projected) {
		output_cols.emplace_back(col);
	}
	for (auto &col : pk_cols) {
		output_cols.emplace_back(col);
	}

	// COUNT(*) projects nothing, and the row count then has to reach us on a batch with no columns
	// at all. Arrow can carry that — length lives on the root array, not its children — but the
	// obvious way to build a result in the host language cannot: a pyarrow Table assembled from an
	// empty array list is zero rows however many rows matched, so the count comes back 0 and nothing
	// reports an error. Asking for one column instead costs that column's data on a query that
	// wanted none, and is the only shape every provider already answers correctly. The extra column
	// is never read: the scan's output has no columns, so only the cardinality survives.
	if (output_cols.empty() && !stream_data->column_names.empty()) {
		output_cols.emplace_back(stream_data->column_names[0]);
	}

	auto filters_json = SerializeFlatFiltersOrThrow(parameters, stream_data->column_names);

	// Scan UDF takes (table_name, columns, filters) and returns the rows as one Arrow IPC stream.
	auto &scan_udf = stream_data->table_name;
	auto &provider_table = stream_data->provider_table_name;
	stream_data->source_conn = make_uniq<Connection>(*stream_data->bridge_info->source_db);
	// Bound, never spliced: filters_json carries string constants straight out of the user's WHERE
	// clause, so a single quote in a predicate value would otherwise close a literal and run as SQL on
	// the SOURCE connection -- past the permission map that is meant to bound what the target reaches.
	// The column list rides as a LIST rather than a joined string for the same reason: a column named
	// with a comma used to split into two names on the Python side.
	auto udf_sql = "SELECT " + KeywordHelper::WriteOptionallyQuoted(scan_udf) + "(?, ?, ?)";
	auto udf_stmt = stream_data->source_conn->Prepare(udf_sql);
	if (udf_stmt->HasError()) {
		udf_stmt->GetErrorObject().Throw("vcat_provider: read UDF is unusable: ");
	}
	// A bound vector<Value>, never the variadic Execute: that overload routes std::string through
	// Value::CreateValue<string>, which builds a BLOB, so a UDF declared VARCHAR never binds.
	vector<Value> udf_params;
	udf_params.emplace_back(provider_table);
	udf_params.push_back(Value::LIST(LogicalType::VARCHAR, std::move(output_cols)));
	udf_params.emplace_back(filters_json);
	auto udf_result = udf_stmt->Execute(udf_params);
	if (udf_result->HasError()) {
		udf_result->GetErrorObject().Throw("vcat_provider: read UDF failed: ");
	}
	auto udf_chunk = udf_result->Fetch();
	if (!udf_chunk || udf_chunk->size() == 0) {
		throw IOException("vcat_provider: read UDF returned no result");
	}
	// Named, not a temporary: StringValue::Get borrows from the Value.
	const Value arrow_value = udf_chunk->GetValue(0, 0);
	if (arrow_value.IsNull()) {
		throw IOException("vcat_provider: read UDF for '%s' returned no Arrow data", provider_table);
	}
	vcat_provider::MakeStreamFromIpc(StringValue::Get(arrow_value), &wrapper->arrow_array_stream,
	                                "vcat_provider: read UDF");

	auto trailing = vcat_provider::DescribeTrailingColumns(
	    *stream_data->source_conn->context, wrapper->arrow_array_stream, pk_cols.size(), "vcat_provider: read UDF");
	stream_data->pk_types = std::move(trailing.types);
	stream_data->pk_formats = std::move(trailing.formats);

	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

struct ProviderScanGlobalState : public ArrowScanGlobalState {
	idx_t pk_start_index = COLUMN_IDENTIFIER_ROW_ID;
	idx_t pk_count = 0;
	shared_ptr<BridgePKBuffer> pk_buffer;
	vector<LogicalType> pk_types;
	vector<string> pk_formats;
};

static unique_ptr<GlobalTableFunctionState> ProviderScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<BridgeScanFunctionData>();
	auto result = make_uniq<ProviderScanGlobalState>();

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

	if (bind_data.owned_stream_data && !bind_data.owned_stream_data->pk_types.empty()) {
		result->pk_types = bind_data.owned_stream_data->pk_types;
		result->pk_formats = bind_data.owned_stream_data->pk_formats;
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

static void EmitProviderScanChunk(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}
	auto &data = data_p.bind_data->CastNoConst<ArrowScanFunctionData>();
	auto &state = data_p.local_state->Cast<ArrowScanLocalState>();
	auto &global_state = data_p.global_state->Cast<ProviderScanGlobalState>();

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
					                                  global_state, state.chunk_offset, output_size, "vcat_provider");
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
					                                  state.chunk_offset, output_size, "vcat_provider");
				}
			}
		}
	}

	output.Verify();
	state.chunk_offset += output.size();
}

static BindInfo ProviderScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<BridgeScanFunctionData>();
	return BindInfo(*data.table);
}

TableFunction ProviderTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	if (!table_info->provider) {
		throw IOException("vcat_provider: provider table '%s' has no provider bound", name);
	}

	auto stream_data = make_uniq<BridgeStreamData>();

	auto bridge_info = make_shared_ptr<BridgeInfo>();
	bridge_info->bridge_id = table_info->provider->probe_id + "::" + table_info->table_name;
	bridge_info->source_db = table_info->db_instance;
	stream_data->bridge_info = bridge_info;
	stream_data->consumed = false;

	for (auto &col : columns.Logical()) {
		stream_data->column_names.push_back(col.GetName());
	}

	stream_data->table_name = table_info->provider->scan_udf;
	stream_data->provider_table_name = table_info->table_name;
	auto producer = reinterpret_cast<stream_factory_produce_t>(RunScanUdfAndDecodeArrowResult);

	auto result = make_uniq<BridgeScanFunctionData>(producer, std::move(stream_data));
	result->projection_pushdown_enabled = true;

	for (auto &col : columns.Logical()) {
		result->all_types.push_back(col.GetType());
	}

	result->table = this;
	bind_data = std::move(result);

	TableFunction scan_func("vcat_provider_scan", {}, EmitProviderScanChunk, nullptr, ProviderScanInitGlobal,
	                        ArrowTableFunction::ArrowScanInitLocal);
	scan_func.projection_pushdown = true;
	scan_func.filter_pushdown = true;
	scan_func.get_bind_info = ProviderScanGetBindInfo;
	return scan_func;
}

TableStorageInfo ProviderTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	return info;
}

} // namespace duckdb
