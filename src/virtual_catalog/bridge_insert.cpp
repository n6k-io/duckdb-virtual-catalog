#include "bridge_insert.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct BridgeInsertGlobalState : public GlobalSinkState {
	vector<unique_ptr<DataChunk>> chunks;
	idx_t affected_rows = 0;
};

struct BridgeInsertSourceState : public GlobalSourceState {
	bool done = false;
};

BridgeInsert::BridgeInsert(PhysicalPlan &physical_plan, BridgeTableCatalogEntry &table_p, vector<LogicalType> types_p,
                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p) {
}

unique_ptr<GlobalSinkState> BridgeInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BridgeInsertGlobalState>();
}

SinkResultType BridgeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeInsertGlobalState>();
	auto copy = make_uniq<DataChunk>();
	copy->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(*copy, 0);
	gstate.affected_rows += chunk.size();
	gstate.chunks.push_back(std::move(copy));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType BridgeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BridgeInsertGlobalState>();

	if (gstate.chunks.empty()) {
		return SinkFinalizeType::READY;
	}

	{
		lock_guard<mutex> lock(table.bridge_info->mtx);
		if (!table.bridge_info->HasWritePermission(table.source_table_name)) {
			throw PermissionException("virtual_catalog: table '%s' is read-only in bridge '%s'",
			                          table.source_table_name, table.bridge_info->bridge_id);
		}
	}

	if (!table.bridge_info->source_db) {
		throw IOException("virtual_catalog: source database for bridge '%s' is unavailable",
		                  table.bridge_info->bridge_id);
	}

	auto conn = make_uniq<Connection>(*table.bridge_info->source_db);
	auto appender = make_uniq<Appender>(*conn, table.bridge_info->source_catalog, table.bridge_info->source_schema,
	                                    table.source_table_name);

	for (auto &chunk : gstate.chunks) {
		appender->AppendDataChunk(*chunk);
	}
	appender->Close();

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> BridgeInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BridgeInsertSourceState>();
}

SourceResultType BridgeInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<BridgeInsertSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<BridgeInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
