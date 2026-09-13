#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

void ShapeWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
