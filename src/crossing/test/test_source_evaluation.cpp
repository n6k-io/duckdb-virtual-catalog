#include "catch.hpp"
#include "memory_source/memory_source.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

using namespace duckdb;

TEST_CASE("an expression with no function call in it crosses", "[evaluation]") {
	MemorySource source;

	REQUIRE(CanEvaluate(*BoundAmt(), source));
	REQUIRE(CanEvaluate(*Int(1), source));
	REQUIRE(CanEvaluate(
	    *make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, BoundAmt(), Int(100)), source));
	REQUIRE(CanEvaluate(*make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, BoundAmt(), Int(1)),
	                    source));
	REQUIRE(CanEvaluate(*make_uniq<BoundBetweenExpression>(BoundAmt(), Int(1), Int(10), true, true), source));
}

TEST_CASE("a cast crosses only if what it casts does", "[evaluation]") {
	DuckDB db(nullptr);
	Connection con(db);
	MemorySource source;

	auto plain = BoundCastExpression::AddCastToType(*con.context, Int(1), LogicalType::BIGINT, true);
	REQUIRE(CanEvaluate(*plain, source));

	auto over_call = BoundCastExpression::AddCastToType(*con.context, Call("target_only"), LogicalType::BIGINT, true);
	REQUIRE(!CanEvaluate(*over_call, source));
}

TEST_CASE("a case crosses only if every branch does", "[evaluation]") {
	MemorySource source;

	auto plain = make_uniq<BoundCaseExpression>(LogicalType::INTEGER);
	plain->case_checks.emplace_back();
	plain->case_checks[0].when_expr =
	    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, BoundAmt(), Int(0));
	plain->case_checks[0].then_expr = Int(1);
	plain->else_expr = Int(0);
	REQUIRE(CanEvaluate(*plain, source));

	auto over_call = make_uniq<BoundCaseExpression>(LogicalType::INTEGER);
	over_call->case_checks.emplace_back();
	over_call->case_checks[0].when_expr = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	over_call->case_checks[0].then_expr = Call("target_only");
	over_call->else_expr = Int(0);
	REQUIRE(!CanEvaluate(*over_call, source));
}

TEST_CASE("a window crosses when the source knows the function and every part of it", "[evaluation]") {
	MemorySource knows {{"row_number"}};
	MemorySource does_not;

	auto row_number = [](unique_ptr<Expression> order_key) {
		auto window =
		    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
		window->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(order_key));
		return window;
	};

	auto verdict = VerdictOn(*row_number(BoundAmt()), knows);
	INFO(verdict.reason);
	REQUIRE(verdict.ok);
	REQUIRE(!CanEvaluate(*row_number(BoundAmt()), does_not));
	REQUIRE(!CanEvaluate(*row_number(Call("target_only")), knows));
}

TEST_CASE("an unnest crosses only if what it unnests does", "[evaluation]") {
	MemorySource source;

	auto plain = make_uniq<BoundUnnestExpression>(LogicalType::INTEGER);
	plain->child = BoundAmt();
	REQUIRE(CanEvaluate(*plain, source));

	auto over_call = make_uniq<BoundUnnestExpression>(LogicalType::INTEGER);
	over_call->child = Call("target_only");
	REQUIRE(!CanEvaluate(*over_call, source));
}

TEST_CASE("an expression kind the whitelist does not name stays", "[evaluation]") {
	MemorySource source;
	auto reference = make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0);

	REQUIRE(!CanEvaluate(*reference, source));
}

TEST_CASE("a function the source knows crosses", "[evaluation]") {
	MemorySource source {{"shift"}};

	REQUIRE(CanEvaluate(*Call("shift"), source));
}

TEST_CASE("a function the source does not know stays", "[evaluation]") {
	MemorySource source {{"shift"}};

	REQUIRE(!CanEvaluate(*Call("target_only"), source));
}

TEST_CASE("a volatile function stays even when the source knows it", "[evaluation]") {
	MemorySource source {{"random"}};

	REQUIRE(!CanEvaluate(*Call("random", FunctionStability::VOLATILE), source));
}

TEST_CASE("an expression stays if any part of it stays", "[evaluation]") {
	MemorySource source;
	auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, Call("target_only"), Int(1));

	REQUIRE(!CanEvaluate(*comparison, source));
}

TEST_CASE("an operator crosses only when all of its expressions do", "[evaluation]") {
	MemorySource source;

	auto evaluable = make_uniq<LogicalFilter>();
	evaluable->expressions.push_back(
	    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, BoundAmt(), Int(100)));
	REQUIRE(CanEvaluateAll(*evaluable, source));

	auto mixed = make_uniq<LogicalFilter>();
	mixed->expressions.push_back(
	    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, BoundAmt(), Int(100)));
	mixed->expressions.push_back(Call("target_only"));
	REQUIRE(!CanEvaluateAll(*mixed, source));
}

TEST_CASE("an expression is column-free only if no part of it names a column", "[evaluation]") {
	REQUIRE(IsColumnFreeExpression(*Int(5)));
	REQUIRE(!IsColumnFreeExpression(*BoundAmt()));
	REQUIRE(!IsColumnFreeExpression(
	    *make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, BoundAmt(), Int(1))));
}
