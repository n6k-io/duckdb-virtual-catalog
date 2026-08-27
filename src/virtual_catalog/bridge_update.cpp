#include "bridge_update.hpp"
#include "bridge_dml.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct BridgeUpdateGlobalState : public GlobalSinkState {
	vector<unique_ptr<DataChunk>> chunks;
	vector<LogicalType> chunk_types;
	idx_t affected_rows = 0;
};

struct BridgeUpdateSourceState : public GlobalSourceState {
	bool done = false;
};

BridgeUpdate::BridgeUpdate(PhysicalPlan &physical_plan, BridgeTableCatalogEntry &table_p,
                           vector<PhysicalIndex> columns_p, vector<unique_ptr<Expression>> expressions_p,
                           vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), update_columns(std::move(columns_p)), update_expressions(std::move(expressions_p)) {
}

unique_ptr<GlobalSinkState> BridgeUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BridgeUpdateGlobalState>();
}

SinkResultType BridgeUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeUpdateGlobalState>();

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

	narrow->data[0].Reference(chunk.data[row_id_col]);

	for (idx_t i = 0; i < update_expressions.size(); i++) {
		auto &binding = update_expressions[i]->Cast<BoundReferenceExpression>();
		narrow->data[i + 1].Reference(chunk.data[binding.index]);
	}

	gstate.chunks.push_back(std::move(narrow));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType BridgeUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeUpdateGlobalState>();

	if (gstate.chunks.empty()) {
		return SinkFinalizeType::READY;
	}

	auto dml = OpenAuthorizedSourceWriteContext(table);
	auto &pk_cols = dml.pk_cols;
	auto &columns = table.GetColumns();

	string set_clause;
	idx_t param_idx = 1;
	for (idx_t i = 0; i < update_columns.size(); i++) {
		if (i > 0) {
			set_clause += ", ";
		}
		auto &col = columns.GetColumn(update_columns[i]);
		set_clause += KeywordHelper::WriteOptionallyQuoted(col.Name()) + " = $" + to_string(param_idx++);
	}

	string where_clause;
	for (idx_t i = 0; i < pk_cols.size(); i++) {
		if (i > 0) {
			where_clause += " AND ";
		}
		where_clause += KeywordHelper::WriteOptionallyQuoted(pk_cols[i]) + " = $" + to_string(param_idx++);
	}

	auto update_sql = "UPDATE " + dml.quoted_table + " SET " + set_clause + " WHERE " + where_clause;
	auto prepared = dml.conn->Prepare(update_sql);
	if (prepared->HasError()) {
		prepared->GetErrorObject().Throw("virtual_catalog: failed to prepare UPDATE: ");
	}

	dml.conn->SendQuery("BEGIN");
	try {
		idx_t total_affected = 0;

		for (auto &chunk : gstate.chunks) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				vector<Value> params;
				for (idx_t i = 0; i < update_columns.size(); i++) {
					params.push_back(chunk->GetValue(i + 1, row));
				}
				auto buf_idx = chunk->GetValue(0, row).GetValue<int64_t>();
				for (auto &v : pk_buffer->rows[buf_idx]) {
					params.push_back(v);
				}

				auto result = prepared->Execute(params);
				if (result->HasError()) {
					result->GetErrorObject().Throw("virtual_catalog: UPDATE on source failed: ");
				}
				auto result_chunk = result->Fetch();
				if (result_chunk && result_chunk->size() > 0) {
					total_affected += result_chunk->GetValue(0, 0).GetValue<int64_t>();
				}
			}
		}

		dml.conn->SendQuery("COMMIT");
		gstate.affected_rows = total_affected;
	} catch (...) {
		dml.conn->SendQuery("ROLLBACK");
		throw;
	}

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> BridgeUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BridgeUpdateSourceState>();
}

SourceResultType BridgeUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<BridgeUpdateSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<BridgeUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
