#include "catch.hpp"
#include "internal/rules.hpp"
#include "memory_source/memory_source.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

namespace {

bool CanCross(LogicalOperator &op) {
	MemorySource source;
	auto rule = CrossingRules::Get().RuleFor(op.type);
	return rule && rule->CanCross(op, source);
}

} // namespace

TEST_CASE("an operator with no rule does not cross", "[rules]") {
	REQUIRE(!CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_DUMMY_SCAN));
	REQUIRE(!CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_GET));
}

TEST_CASE("a filter crosses when the source can compute its predicates", "[rules]") {
	auto evaluable = make_uniq<LogicalFilter>();
	evaluable->expressions.push_back(
	    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, BoundAmt(), Int(100)));
	REQUIRE(CanCross(*evaluable));

	auto unevaluable = make_uniq<LogicalFilter>();
	unevaluable->expressions.push_back(Call("target_only"));
	REQUIRE(!CanCross(*unevaluable));
}

TEST_CASE("a constant limit crosses", "[rules]") {
	LogicalLimit limit(BoundLimitNode::ConstantValue(5), BoundLimitNode());

	REQUIRE(CanCross(limit));
}

TEST_CASE("a limit whose bound names a column does not cross", "[rules]") {
	LogicalLimit limit(BoundLimitNode::ExpressionValue(BoundAmt()), BoundLimitNode());

	REQUIRE(!CanCross(limit));
}

TEST_CASE("a percentage limit does not cross", "[rules]") {
	LogicalLimit limit(BoundLimitNode::ConstantPercentage(10.0), BoundLimitNode());

	REQUIRE(!CanCross(limit));
}

TEST_CASE("a join crosses, and the labelling decides whether its branches agree", "[rules]") {
	LogicalComparisonJoin join(JoinType::INNER);

	REQUIRE(CanCross(join));
}

TEST_CASE("an aggregate crosses now that the scan above it is rebuilt", "[rules]") {
	REQUIRE(CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_WINDOW));
	REQUIRE(CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_UNNEST));
	REQUIRE(CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_PIVOT));
}

TEST_CASE("an order by crosses when the source can compute its keys", "[rules]") {
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, BoundAmt());
	LogicalOrder order(std::move(orders));

	REQUIRE(CanCross(order));
}

TEST_CASE("an order by whose keys the source cannot compute does not cross", "[rules]") {
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, Call("target_only"));
	LogicalOrder order(std::move(orders));

	REQUIRE(!CanCross(order));
}

TEST_CASE("what belongs to the target never crosses", "[rules]") {
	MemorySource source;
	auto never = {LogicalOperatorType::LOGICAL_INSERT,       LogicalOperatorType::LOGICAL_UPDATE,
	              LogicalOperatorType::LOGICAL_DELETE,       LogicalOperatorType::LOGICAL_COPY_TO_FILE,
	              LogicalOperatorType::LOGICAL_CREATE_TABLE, LogicalOperatorType::LOGICAL_ATTACH,
	              LogicalOperatorType::LOGICAL_SET,          LogicalOperatorType::LOGICAL_CREATE_SECRET,
	              LogicalOperatorType::LOGICAL_EXPLAIN,      LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR};

	for (auto type : never) {
		auto rule = CrossingRules::Get().RuleFor(type);
		REQUIRE(rule);
		LogicalProjection op(40, vector<unique_ptr<Expression>>());
		REQUIRE(!rule->CanCross(op, source));
	}
}

TEST_CASE("rows the target holds are the leaves a write may cross to", "[rules]") {
	REQUIRE(IsMaterialisedRows(LogicalOperatorType::LOGICAL_CHUNK_GET));
	REQUIRE(IsMaterialisedRows(LogicalOperatorType::LOGICAL_EXPRESSION_GET));
	REQUIRE(!IsMaterialisedRows(LogicalOperatorType::LOGICAL_DUMMY_SCAN));
	REQUIRE(!CrossingRules::Get().RuleFor(LogicalOperatorType::LOGICAL_CHUNK_GET));
}
