#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "bridge_table_entry.hpp"
#include "bridge_table_function.hpp"

namespace duckdb {

class BridgeDelete : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	BridgeDelete(PhysicalPlan &physical_plan, BridgeTableCatalogEntry &table, idx_t row_id_index,
	             vector<LogicalType> types, idx_t estimated_cardinality);

	BridgeTableCatalogEntry &table;
	idx_t row_id_index;
	shared_ptr<BridgePKBuffer> pk_buffer;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
