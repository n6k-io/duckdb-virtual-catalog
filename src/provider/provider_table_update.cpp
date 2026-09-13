#include "provider_table_update.hpp"
#include "provider_arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

struct ProviderUpdateGlobalState : public GlobalSinkState {
	vector<unique_ptr<DataChunk>> chunks;
	vector<LogicalType> chunk_types;
	idx_t affected_rows = 0;
};

struct ProviderUpdateSourceState : public GlobalSourceState {
	bool done = false;
};

ProviderTableUpdate::ProviderTableUpdate(PhysicalPlan &physical_plan, ProviderTableCatalogEntry &table_p,
                                         vector<PhysicalIndex> columns_p, vector<unique_ptr<Expression>> expressions_p,
                                         vector<unique_ptr<Expression>> bound_defaults_p, vector<LogicalType> types_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), update_columns(std::move(columns_p)), update_expressions(std::move(expressions_p)),
      bound_defaults(std::move(bound_defaults_p)) {
}

unique_ptr<GlobalSinkState> ProviderTableUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ProviderUpdateGlobalState>();
}

SinkResultType ProviderTableUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderUpdateGlobalState>();

	if (gstate.chunk_types.empty()) {
		gstate.chunk_types.push_back(LogicalType::BIGINT);
		auto &columns = table.GetColumns();
		for (auto &col_idx : update_columns) {
			gstate.chunk_types.push_back(columns.GetColumn(col_idx).Type());
		}
	}

	idx_t row_id_col = chunk.ColumnCount() - 1;

	auto narrow = make_uniq<DataChunk>();
	narrow->Initialize(Allocator::DefaultAllocator(), gstate.chunk_types);
	narrow->SetCardinality(chunk.size());
	// Copy, not Reference: these chunks are held until Finalize and the child re-fills its output
	// chunk in place, so a reference would leave every stored chunk showing the last morsel.
	VectorOperations::Copy(chunk.data[row_id_col], narrow->data[0], chunk.size(), 0, 0);

	// A DEFAULT target binds to a BoundDefaultExpression, which carries no index into the child
	// chunk, so its value is evaluated here. The same split PhysicalUpdate::Sink makes.
	ExpressionExecutor default_executor(context.client, bound_defaults);
	default_executor.SetChunk(chunk);
	for (idx_t i = 0; i < update_expressions.size(); i++) {
		if (update_expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			default_executor.ExecuteExpression(update_columns[i].index, narrow->data[i + 1]);
			continue;
		}
		auto &binding = update_expressions[i]->Cast<BoundReferenceExpression>();
		VectorOperations::Copy(chunk.data[binding.index], narrow->data[i + 1], chunk.size(), 0, 0);
	}

	gstate.chunks.push_back(std::move(narrow));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType ProviderTableUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderUpdateGlobalState>();

	if (gstate.chunks.empty()) {
		return SinkFinalizeType::READY;
	}

	if (!table.table_info->db_instance) {
		throw IOException("virtual_catalog_provider: database instance unavailable");
	}

	if (!table.table_info->provider) {
		throw IOException("virtual_catalog_provider: provider table '%s' has no provider bound", table.name);
	}

	auto &pk_cols = table.table_info->primary_keys;
	auto &all_columns = table.GetColumns();

	auto conn = make_uniq<Connection>(*table.table_info->db_instance);

	// The row layout the provider is handed: every key column, then only the changed ones.
	// `changed_col_names` names that second half so the provider knows where the keys stop.
	vector<LogicalType> row_types;
	vector<string> row_col_names;
	for (auto &pk : pk_cols) {
		for (auto &col : all_columns.Logical()) {
			if (col.GetName() == pk) {
				row_types.push_back(col.GetType());
				row_col_names.push_back(pk);
				break;
			}
		}
	}

	vector<string> changed_col_names;
	for (auto &col_idx : update_columns) {
		auto &col = all_columns.GetColumn(col_idx);
		row_types.push_back(col.GetType());
		row_col_names.push_back(col.Name());
		changed_col_names.push_back(col.Name());
	}

	// Each sunk chunk holds a PK-buffer row id in column 0 and the new values after it; the keys
	// themselves live in the buffer, so the two halves are stitched back into one row here.
	vcat_provider::RowChunkBuilder builder(row_types);
	idx_t rows_sent = 0;
	for (auto &chunk : gstate.chunks) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			rows_sent++;
			auto buf_idx = chunk->GetValue(0, row).GetValue<int64_t>();
			builder.BeginRow();
			for (auto &pk_value :
			     PKBufferRow(pk_buffer.get(), buf_idx, pk_cols.size(), "virtual_catalog_provider: update")) {
				builder.Append(pk_value);
			}
			for (idx_t i = 0; i < update_columns.size(); i++) {
				builder.Append(chunk->GetValue(i + 1, row));
			}
			builder.EndRow("virtual_catalog_provider: update");
		}
	}

	string changed_cols_str;
	for (idx_t i = 0; i < changed_col_names.size(); i++) {
		if (i > 0) {
			changed_cols_str += ",";
		}
		changed_cols_str += changed_col_names[i];
	}

	auto chunks = builder.Finish();
	auto arrow_ipc = vcat_provider::EncodeChunksAsIpc(*conn->context, row_types, row_col_names, chunks);
	auto affected = NumericCast<idx_t>(vcat_provider::CallWriteUdf(
	    *conn, table.table_info->provider->update_udf, table.table_info->table_name, arrow_ipc,
	    vector<Value> {Value(changed_cols_str)}, "virtual_catalog_provider: update UDF"));
	// The UDF has already run and nothing here can undo it: this reports, it does not prevent.
	EnsureKeyIsUnique(affected, rows_sent, table.table_info->table_name, pk_cols, "UPDATE", false);
	gstate.affected_rows = affected;

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> ProviderTableUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ProviderUpdateSourceState>();
}

SourceResultType ProviderTableUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<ProviderUpdateSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<ProviderUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
