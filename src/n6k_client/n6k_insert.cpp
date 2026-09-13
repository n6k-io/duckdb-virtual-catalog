#include "n6k_insert.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_fetch.hpp"
#include "n6k_catalog.hpp"
#include "n6k_str_utils.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

namespace duckdb {

struct N6kInsertGlobalState : public GlobalSinkState {
	vector<unique_ptr<DataChunk>> chunks;
	idx_t affected_rows = 0;
	optional_ptr<N6kTableCatalogEntry> created_table;
};

struct N6kInsertSourceState : public GlobalSourceState {
	bool done = false;
};

N6kInsert::N6kInsert(PhysicalPlan &physical_plan, N6kTableCatalogEntry &table_p, vector<LogicalType> types_p,
                     idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(&table_p) {
}

N6kInsert::N6kInsert(PhysicalPlan &physical_plan, N6kSchemaEntry &schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                     vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      schema_entry(&schema_p), create_info(std::move(info_p)) {
}

unique_ptr<GlobalSinkState> N6kInsert::GetGlobalSinkState(ClientContext &context) const {
	auto gstate = make_uniq<N6kInsertGlobalState>();

	if (create_info) {
		auto &schema = const_cast<N6kSchemaEntry &>(*schema_entry); // NOLINT(cppcoreguidelines-pro-type-const-cast)
		auto catalog_transaction = schema.GetCatalogTransaction(context);
		auto result = schema.CreateTable(catalog_transaction, *create_info);
		if (result) {
			gstate->created_table = &result->Cast<N6kTableCatalogEntry>();
		}
	}

	return std::move(gstate);
}

SinkResultType N6kInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<N6kInsertGlobalState>();
	auto copy = make_uniq<DataChunk>();
	copy->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	chunk.Copy(*copy, 0);
	gstate.affected_rows += chunk.size();
	gstate.chunks.push_back(std::move(copy));
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType N6kInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                     OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<N6kInsertGlobalState>();

	if (gstate.chunks.empty()) {
		return SinkFinalizeType::READY;
	}

	auto *target = table.get();
	if (!target && gstate.created_table) {
		target = gstate.created_table.get();
	}
	if (!target) {
		throw IOException("n6k: no target table for INSERT");
	}

	auto col_types = target->GetTypes();
	auto &columns = target->GetColumns();
	vector<string> col_names;
	for (auto &col : columns.Logical()) {
		col_names.push_back(col.Name());
	}

	ArrowBuffer ipc_buf;
	ArrowBufferInit(&ipc_buf);
	SerializeChunksToArrowIPC(context, col_types, col_names, gstate.chunks, &ipc_buf);

	// Every N6kTableCatalogEntry is built by N6kSchemaEntry with the catalog's session, and an attached
	// catalog cannot have a null one, so `session` is never null here.
	target->session->Insert(target->table_schema, target->table_name, static_cast<const uint8_t *>(ipc_buf.data),
	                        static_cast<size_t>(ipc_buf.size_bytes));

	ArrowBufferReset(&ipc_buf);

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> N6kInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<N6kInsertSourceState>();
}

SourceResultType N6kInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<N6kInsertSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	auto &gstate = sink_state->Cast<N6kInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
