#include "provider_table_delete.hpp"
#include "provider_arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

struct ProviderDeleteGlobalState : public GlobalSinkState {
	vector<Value> id_values;
	idx_t affected_rows = 0;
};

struct ProviderDeleteSourceState : public GlobalSourceState {
	bool done = false;
};

ProviderTableDelete::ProviderTableDelete(PhysicalPlan &physical_plan, ProviderTableCatalogEntry &table_p,
                                         idx_t row_id_index_p, vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), row_id_index(row_id_index_p) {
}

unique_ptr<GlobalSinkState> ProviderTableDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ProviderDeleteGlobalState>();
}

SinkResultType ProviderTableDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderDeleteGlobalState>();
	for (idx_t i = 0; i < chunk.size(); i++) {
		gstate.id_values.push_back(chunk.GetValue(row_id_index, i));
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType ProviderTableDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderDeleteGlobalState>();

	if (gstate.id_values.empty()) {
		return SinkFinalizeType::READY;
	}

	if (!table.table_info->db_instance) {
		throw IOException("virtual_catalog_provider: database instance unavailable");
	}

	if (!table.table_info->provider) {
		throw IOException("virtual_catalog_provider: provider table '%s' has no provider bound", table.name);
	}

	auto &pk_cols = table.table_info->primary_keys;
	auto conn = make_uniq<Connection>(*table.table_info->db_instance);

	vector<LogicalType> pk_types;
	for (auto &pk_col : pk_cols) {
		for (auto &col : table.GetColumns().Logical()) {
			if (col.GetName() == pk_col) {
				pk_types.push_back(col.GetType());
				break;
			}
		}
	}

	// The rows to delete are key VALUES held in the PK buffer, not a relation, so they are gathered
	// into chunks here before the Arrow encoder can see them.
	vcat_provider::RowChunkBuilder builder(pk_types);
	for (auto &id_val : gstate.id_values) {
		auto buf_idx = id_val.GetValue<int64_t>();
		builder.BeginRow();
		for (auto &pk_value :
		     PKBufferRow(pk_buffer.get(), buf_idx, pk_cols.size(), "virtual_catalog_provider: delete")) {
			builder.Append(pk_value);
		}
		builder.EndRow("virtual_catalog_provider: delete");
	}
	auto chunks = builder.Finish();
	auto arrow_ipc = vcat_provider::EncodeChunksAsIpc(*conn->context, pk_types, pk_cols, chunks);
	auto affected = NumericCast<idx_t>(
	    vcat_provider::CallWriteUdf(*conn, table.table_info->provider->delete_udf, table.table_info->table_name,
	                                arrow_ipc, vector<Value>(), "virtual_catalog_provider: delete UDF"));
	// The UDF has already run and nothing here can undo it: this reports, it does not prevent.
	EnsureKeyIsUnique(affected, gstate.id_values.size(), table.table_info->table_name, pk_cols, "DELETE", false);
	gstate.affected_rows = affected;

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> ProviderTableDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ProviderDeleteSourceState>();
}

SourceResultType ProviderTableDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<ProviderDeleteSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<ProviderDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
