#include "predicate_fold.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

namespace {

bool IsConstantTrue(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &value = expr.Cast<BoundConstantExpression>().value;
	return !value.IsNull() && value.type().id() == LogicalTypeId::BOOLEAN && BooleanValue::Get(value);
}

bool OnlyTrueFilters(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_GET:
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		for (auto &expr : op.expressions) {
			if (!IsConstantTrue(*expr)) {
				return false;
			}
		}
		break;
	default:
		return false;
	}
	for (auto &child : op.children) {
		if (!OnlyTrueFilters(*child)) {
			return false;
		}
	}
	return true;
}

} // namespace

bool PredicateFoldsToTrue(ClientContext &context, const string &catalog, const string &schema, const string &table,
                          const ParsedExpression &predicate) {
	auto from = make_uniq<BaseTableRef>();
	from->catalog_name = catalog;
	from->schema_name = schema;
	from->table_name = table;
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = std::move(from);
	node->where_clause = predicate.Copy();
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);

	Planner planner(context);
	planner.CreatePlan(std::move(select));
	Optimizer optimizer(*planner.binder, context);
	optimizer.rewriter.VisitOperator(*planner.plan);
	return OnlyTrueFilters(*planner.plan);
}

} // namespace duckdb
