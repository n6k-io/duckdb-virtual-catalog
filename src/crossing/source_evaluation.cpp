#include "internal/source_evaluation.hpp"

#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

CrossingVerdict VerdictOn(const Expression &expr, CrossingSource &source) {
	auto type = source.AcceptsType(expr.return_type);
	if (!type.ok) {
		return type;
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_COMPARISON:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CAST:
	case ExpressionClass::BOUND_BETWEEN:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_UNNEST:
		break;
	case ExpressionClass::BOUND_FUNCTION:
	case ExpressionClass::BOUND_AGGREGATE:
	case ExpressionClass::BOUND_WINDOW: {
		auto call = source.AcceptsCall(expr);
		if (!call.ok) {
			return call;
		}
		break;
	}
	default:
		return CrossingVerdict::No("crossing does not move a " + ExpressionClassToString(expr.GetExpressionClass()));
	}
	// A volatile or inconsistent expression evaluated once here and once on the source gives two
	// different answers for the one expression.
	if (expr.IsVolatile() || !expr.IsConsistent()) {
		return CrossingVerdict::No(expr.ToString() + " would be evaluated twice");
	}
	CrossingVerdict verdict;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (verdict.ok) {
			verdict = VerdictOn(child, source);
		}
	});
	return verdict;
}

bool CanEvaluate(const Expression &expr, CrossingSource &source) {
	return VerdictOn(expr, source).ok;
}

bool CanEvaluateAll(LogicalOperator &op, CrossingSource &source) {
	bool evaluable = true;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *child) { evaluable = evaluable && CanEvaluate(**child, source); });
	return evaluable;
}

bool IsColumnFreeExpression(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	bool column_free = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { column_free = column_free && IsColumnFreeExpression(child); });
	return column_free;
}

} // namespace duckdb
