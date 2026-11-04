#include "catch.hpp"
#include "filter_json.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"

using namespace duckdb;
using namespace duckdb::filter_json;

namespace {

struct SerializeCase {
	TableFilterSet filters;
	unordered_map<idx_t, idx_t> filter_to_col;
	vector<string> column_names;
	FilterSerializeResult result;

	string Run() {
		return SerializeFilters(filters, filter_to_col, column_names, result);
	}
};

unique_ptr<TableFilter> Constant(ExpressionType type, Value val) {
	return ExpressionFilter::CreateComparisonFilter(type, std::move(val));
}

unique_ptr<Expression> Column(const LogicalType &type = LogicalType::INTEGER) {
	return make_uniq<BoundReferenceExpression>(type, 0ULL);
}

unique_ptr<Expression> Const(Value val) {
	return make_uniq<BoundConstantExpression>(std::move(val));
}

unique_ptr<Expression> Or() {
	return make_uniq<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_OR,
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Column(), Const(Value::INTEGER(1))),
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Column(), Const(Value::INTEGER(2))));
}

unique_ptr<TableFilter> Filter(unique_ptr<Expression> expr) {
	return make_uniq<ExpressionFilter>(std::move(expr));
}

} // namespace

TEST_CASE("filter_json: constant comparison renders a 3-element clause", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(42)));

	REQUIRE(c.Run() == R"([["a",">",42]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: string values render as JSON strings with no tag", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"name"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value("bob")));

	REQUIRE(c.Run() == R"([["name","=","bob"]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a value that crosses as text carries a rebuild tag", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"d"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value::DATE(date_t(0))));

	REQUIRE(c.Run() == R"([["d","=","1970-01-01","date"]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: null checks render without a value", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Filter(ExpressionFilter::CreateNullCheckExpression(
	                                             Column(), ExpressionType::OPERATOR_IS_NULL)));

	REQUIRE(c.Run() == R"([["a","is_null",null]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: IN renders one clause with a shared tag", "[filter_json]") {
	vector<Value> values {Value::BIGINT(1), Value::BIGINT(2)};
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0),
	                     Filter(ExpressionFilter::CreateInExpression(Column(values[0].type()), values)));

	REQUIRE(c.Run() == R"([["a","in",[1,2]]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: an unmapped filter is unsupported, not silently dropped", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value::INTEGER(1)));

	c.Run();
	REQUIRE_FALSE(c.result.all_exact);
	REQUIRE(c.result.first_unsupported == "?");
}

TEST_CASE("filter_json: an unrenderable child poisons its conjunction", "[filter_json]") {
	auto conj = make_uniq<BoundConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND,
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, Column(), Const(Value::INTEGER(1))),
	    Or());

	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Filter(std::move(conj)));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
	REQUIRE(c.result.first_unsupported == "a");
}

TEST_CASE("filter_json: an optional filter with an unrenderable child is skipped", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Filter(CreateOptionalFilterExpression(Or(), LogicalType::INTEGER)));

	REQUIRE(c.Run().empty());
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: an infinite value renders as a bare token", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"d"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_GREATERTHAN,
	                                                  Value::DOUBLE(std::numeric_limits<double>::infinity())));

	REQUIRE(c.Run() == R"([["d",">",Infinity]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a NaN comparison is unsupported", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"d"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Constant(ExpressionType::COMPARE_LESSTHAN,
	                                                  Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
	REQUIRE(c.result.first_unsupported == "d");
}

TEST_CASE("filter_json: a NaN inside IN is unsupported", "[filter_json]") {
	vector<Value> values {Value::DOUBLE(1.5), Value::DOUBLE(std::numeric_limits<double>::quiet_NaN())};
	SerializeCase c;
	c.column_names = {"d"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0),
	                     Filter(ExpressionFilter::CreateInExpression(Column(values[0].type()), values)));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
}

TEST_CASE("filter_json: a constant on the left flips the comparison", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0),
	                     Filter(BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN,
	                                                              Const(Value::INTEGER(5)), Column())));

	REQUIRE(c.Run() == R"([["a","<",5]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a mandatory OR is unsupported", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Filter(Or()));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
}

TEST_CASE("filter_json: a comparison with no constant is unsupported", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0),
	                     Filter(BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Column(), Column())));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
}

TEST_CASE("filter_json: a renderable optional filter still narrows the scan", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0), Filter(CreateOptionalFilterExpression(
	                                             BoundComparisonExpression::Create(ExpressionType::COMPARE_LESSTHAN,
	                                                                               Column(), Const(Value::INTEGER(9))),
	                                             LogicalType::INTEGER)));

	REQUIRE(c.Run() == R"([["a","<",9]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a dynamic filter is a hint and is skipped", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ProjectionIndex(0),
	                     Filter(CreateDynamicFilterExpression(
	                         make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)),
	                         LogicalType::INTEGER)));

	REQUIRE(c.Run().empty());
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a mandatory multi-column filter is unsupported", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a", "b"};
	c.filter_to_col[0] = 0;
	c.filter_to_col[1] = 1;
	c.filters.PushMultiColumnFilter(make_uniq<ExpressionFilter>(
	    BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, Column(),
	                                      make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 1ULL)),
	    vector<ProjectionIndex> {ProjectionIndex(0), ProjectionIndex(1)}));

	c.Run();
	REQUIRE_FALSE(c.result.all_exact);
}

TEST_CASE("filter_json: ValueTypeTag", "[filter_json]") {
	CHECK(ValueTypeTag(Value::INTEGER(1)) == nullptr);
	CHECK(ValueTypeTag(Value("s")) == nullptr);
	CHECK(string(ValueTypeTag(Value::TIMESTAMP(timestamp_t(0)))) == "timestamp");
	CHECK(string(ValueTypeTag(Value::HUGEINT(hugeint_t(1)))) == "int");
	CHECK(string(ValueTypeTag(Value::BLOB("x"))) == "opaque");
	CHECK(ValueTypeTag(Value(LogicalType::INTEGER)) == nullptr);
}
