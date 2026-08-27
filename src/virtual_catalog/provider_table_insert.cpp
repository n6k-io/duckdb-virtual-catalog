#include "provider_table_insert.hpp"
#include "provider_arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

struct ProviderInsertGlobalState : public GlobalSinkState {
	vector<unique_ptr<DataChunk>> chunks;
	idx_t affected_rows = 0;
};

struct ProviderInsertSourceState : public GlobalSourceState {
	bool done = false;
};

ProviderTableInsert::ProviderTableInsert(PhysicalPlan &physical_plan, ProviderTableCatalogEntry &table_p,
                                         vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p) {
}

unique_ptr<GlobalSinkState> ProviderTableInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ProviderInsertGlobalState>();
}

SinkResultType ProviderTableInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderInsertGlobalState>();
	auto copy = make_uniq<DataChunk>();
	copy->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(*copy, 0);
	gstate.affected_rows += chunk.size();
	gstate.chunks.push_back(std::move(copy));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType ProviderTableInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<ProviderInsertGlobalState>();

	if (gstate.chunks.empty()) {
		return SinkFinalizeType::READY;
	}

	if (!table.table_info->db_instance) {
		throw IOException("vcat_provider: database instance unavailable for provider table '%s'", table.name);
	}
	if (!table.table_info->provider) {
		throw IOException("vcat_provider: provider table '%s' has no provider bound", table.name);
	}

	auto conn = make_uniq<Connection>(*table.table_info->db_instance);

	vector<LogicalType> types;
	vector<string> names;
	for (auto &col : table.GetColumns().Logical()) {
		types.push_back(col.GetType());
		names.push_back(col.GetName());
	}

	auto arrow_ipc = vcat_provider::EncodeChunksAsIpc(*conn->context, types, names, gstate.chunks);
	gstate.affected_rows = NumericCast<idx_t>(vcat_provider::CallWriteUdf(*conn, table.table_info->provider->insert_udf,
	                                                                     table.table_info->table_name, arrow_ipc,
	                                                                     vector<Value>(), "vcat_provider: insert UDF"));

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> ProviderTableInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ProviderInsertSourceState>();
}

SourceResultType ProviderTableInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<ProviderInsertSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<ProviderInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
