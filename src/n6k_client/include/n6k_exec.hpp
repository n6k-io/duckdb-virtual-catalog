#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "n6k_table_entry.hpp"

namespace duckdb {

class N6kExec : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	N6kExec(PhysicalPlan &physical_plan, N6kTableCatalogEntry &table, string sql, vector<LogicalType> types,
	        idx_t estimated_cardinality);

	N6kTableCatalogEntry &table;
	string sql;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
