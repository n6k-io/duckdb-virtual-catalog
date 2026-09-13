#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "provider_table_entry.hpp"
#include "vcat_pk_buffer.hpp"

namespace duckdb {

class ProviderTableUpdate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	ProviderTableUpdate(PhysicalPlan &physical_plan, ProviderTableCatalogEntry &table, vector<PhysicalIndex> columns,
	                    vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> bound_defaults,
	                    vector<LogicalType> types, idx_t estimated_cardinality);

	ProviderTableCatalogEntry &table;
	vector<PhysicalIndex> update_columns;
	vector<unique_ptr<Expression>> update_expressions;
	//! Per table column, indexed by PhysicalIndex; drives the VALUE_DEFAULT entries of update_expressions.
	vector<unique_ptr<Expression>> bound_defaults;
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
