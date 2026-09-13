#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/source.hpp"

namespace duckdb {

void CrossingMoveWorkPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
