#include "catch.hpp"
#include "filter_json.hpp"

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
	return make_uniq<ConstantFilter>(type, std::move(val));
}

} // namespace

TEST_CASE("filter_json: constant comparison renders a 3-element clause", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), Constant(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(42)));

	REQUIRE(c.Run() == R"([["a",">",42]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: string values render as JSON strings with no tag", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"name"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value("bob")));

	REQUIRE(c.Run() == R"([["name","=","bob"]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: a value that crosses as text carries a rebuild tag", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"d"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value::DATE(date_t(0))));

	REQUIRE(c.Run() == R"([["d","=","1970-01-01","date"]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: null checks render without a value", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), make_uniq<IsNullFilter>());

	REQUIRE(c.Run() == R"([["a","is_null",null]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: IN renders one clause with a shared tag", "[filter_json]") {
	vector<Value> values {Value::BIGINT(1), Value::BIGINT(2)};
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), make_uniq<InFilter>(std::move(values)));

	REQUIRE(c.Run() == R"([["a","in",[1,2]]])");
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: an unmapped filter is unsupported, not silently dropped", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filters.PushFilter(ColumnIndex(0), Constant(ExpressionType::COMPARE_EQUAL, Value::INTEGER(1)));

	c.Run();
	REQUIRE_FALSE(c.result.all_exact);
	REQUIRE(c.result.first_unsupported == "?");
}

TEST_CASE("filter_json: an unrenderable child poisons its conjunction", "[filter_json]") {
	auto conj = make_uniq<ConjunctionAndFilter>();
	conj->child_filters.push_back(Constant(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(1)));
	conj->child_filters.push_back(make_uniq<ConjunctionOrFilter>());

	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), std::move(conj));

	REQUIRE(c.Run().empty());
	REQUIRE_FALSE(c.result.all_exact);
	REQUIRE(c.result.first_unsupported == "a");
}

TEST_CASE("filter_json: an optional filter with an unrenderable child is skipped", "[filter_json]") {
	SerializeCase c;
	c.column_names = {"a"};
	c.filter_to_col[0] = 0;
	c.filters.PushFilter(ColumnIndex(0), make_uniq<OptionalFilter>(make_uniq<ConjunctionOrFilter>()));

	REQUIRE(c.Run().empty());
	REQUIRE(c.result.all_exact);
}

TEST_CASE("filter_json: ValueTypeTag", "[filter_json]") {
	CHECK(ValueTypeTag(Value::INTEGER(1)) == nullptr);
	CHECK(ValueTypeTag(Value("s")) == nullptr);
	CHECK(string(ValueTypeTag(Value::TIMESTAMP(timestamp_t(0)))) == "timestamp");
	CHECK(string(ValueTypeTag(Value::HUGEINT(hugeint_t(1)))) == "int");
	CHECK(string(ValueTypeTag(Value::BLOB("x"))) == "opaque");
	CHECK(ValueTypeTag(Value(LogicalType::INTEGER)) == nullptr);
}
