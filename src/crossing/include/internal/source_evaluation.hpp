#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "internal/source.hpp"

namespace duckdb {

//! The expression classes are a whitelist: a kind nobody has considered does not cross by default.
CrossingVerdict VerdictOn(const Expression &expr, CrossingSource &source);

bool CanEvaluate(const Expression &expr, CrossingSource &source);

bool CanEvaluateAll(LogicalOperator &op, CrossingSource &source);

bool IsColumnFreeExpression(const Expression &expr);

} // namespace duckdb
