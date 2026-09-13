#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

idx_t FreshTableIndexBase();

void OffsetTableIndices(LogicalOperator &op, idx_t offset);

} // namespace duckdb
