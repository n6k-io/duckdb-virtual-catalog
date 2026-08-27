#include "bridge_delete.hpp"
#include "bridge_dml.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct BridgeDeleteGlobalState : public GlobalSinkState {
	vector<Value> id_values; // ROW_ID slot values (buffer indices or direct rowids)
	idx_t affected_rows = 0;
};

struct BridgeDeleteSourceState : public GlobalSourceState {
	bool done = false;
};

BridgeDelete::BridgeDelete(PhysicalPlan &physical_plan, BridgeTableCatalogEntry &table_p, idx_t row_id_index_p,
                           vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), row_id_index(row_id_index_p) {
}

unique_ptr<GlobalSinkState> BridgeDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BridgeDeleteGlobalState>();
}

SinkResultType BridgeDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeDeleteGlobalState>();
	for (idx_t i = 0; i < chunk.size(); i++) {
		gstate.id_values.push_back(chunk.GetValue(row_id_index, i));
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType BridgeDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeDeleteGlobalState>();

	if (gstate.id_values.empty()) {
		return SinkFinalizeType::READY;
	}

	auto dml = OpenAuthorizedSourceWriteContext(table);
	auto &pk_cols = dml.pk_cols;

	dml.conn->SendQuery("BEGIN");
	try {
		idx_t total_affected = 0;

		if (pk_cols.size() == 1) {
			auto pk_col_quoted = KeywordHelper::WriteOptionallyQuoted(pk_cols[0]);

			idx_t offset = 0;
			while (offset < gstate.id_values.size()) {
				idx_t batch_end = MinValue<idx_t>(offset + 1000, gstate.id_values.size());
				idx_t batch_size = batch_end - offset;

				string placeholders;
				for (idx_t j = 0; j < batch_size; j++) {
					if (j > 0) {
						placeholders += ", ";
					}
					placeholders += "$" + to_string(j + 1);
				}
				auto sql = "DELETE FROM " + dml.quoted_table + " WHERE " + pk_col_quoted + " IN (" + placeholders + ")";
				auto prepared = dml.conn->Prepare(sql);
				if (prepared->HasError()) {
					prepared->GetErrorObject().Throw("virtual_catalog: failed to prepare DELETE: ");
				}

				vector<Value> params;
				for (idx_t j = 0; j < batch_size; j++) {
					auto buf_idx = gstate.id_values[offset + j].GetValue<int64_t>();
					params.push_back(pk_buffer->rows[buf_idx][0]);
				}
				auto result = prepared->Execute(params);
				if (result->HasError()) {
					result->GetErrorObject().Throw("virtual_catalog: DELETE on source failed: ");
				}
				auto chunk = result->Fetch();
				if (chunk && chunk->size() > 0) {
					total_affected += chunk->GetValue(0, 0).GetValue<int64_t>();
				}

				offset = batch_end;
			}
		} else {
			string where_clause;
			idx_t param_idx = 1;
			for (idx_t i = 0; i < pk_cols.size(); i++) {
				if (i > 0) {
					where_clause += " AND ";
				}
				where_clause += KeywordHelper::WriteOptionallyQuoted(pk_cols[i]) + " = $" + to_string(param_idx++);
			}
			auto delete_sql = "DELETE FROM " + dml.quoted_table + " WHERE " + where_clause;
			auto prepared = dml.conn->Prepare(delete_sql);
			if (prepared->HasError()) {
				prepared->GetErrorObject().Throw("virtual_catalog: failed to prepare DELETE: ");
			}

			for (auto &id_val : gstate.id_values) {
				auto buf_idx = id_val.GetValue<int64_t>();
				auto &pk_vals = pk_buffer->rows[buf_idx];
				auto result = prepared->Execute(pk_vals);
				if (result->HasError()) {
					result->GetErrorObject().Throw("virtual_catalog: DELETE on source failed: ");
				}
				auto chunk = result->Fetch();
				if (chunk && chunk->size() > 0) {
					total_affected += chunk->GetValue(0, 0).GetValue<int64_t>();
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

unique_ptr<GlobalSourceState> BridgeDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BridgeDeleteSourceState>();
}

SourceResultType BridgeDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<BridgeDeleteSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<BridgeDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
