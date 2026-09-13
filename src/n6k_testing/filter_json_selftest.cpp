#include "filter_json_selftest.hpp"
#include "filter_json.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"

namespace duckdb {

namespace {

struct SelftestRow {
	string name;
	string json;
	bool exact;
};

unique_ptr<TableFilter> Cmp(ExpressionType type, Value constant) {
	return make_uniq<ConstantFilter>(type, std::move(constant));
}

// COMPARE_DISTINCT_FROM has no n6k wire operator, so it is the cheapest stand-in for "a filter the
// serializer cannot render" without having to build a bound Expression for an EXPRESSION_FILTER.
unique_ptr<TableFilter> Unrenderable() {
	return Cmp(ExpressionType::COMPARE_DISTINCT_FROM, Value::INTEGER(1));
}

unique_ptr<TableFilter> Conjunction(bool is_and, vector<unique_ptr<TableFilter>> children) {
	unique_ptr<ConjunctionFilter> node;
	if (is_and) {
		node = make_uniq<ConjunctionAndFilter>();
	} else {
		node = make_uniq<ConjunctionOrFilter>();
	}
	for (auto &child : children) {
		node->child_filters.push_back(std::move(child));
	}
	return std::move(node);
}

SelftestRow Run(const string &name, unique_ptr<TableFilter> filter, bool allow_nested, bool tag_values = false) {
	TableFilterSet filter_set;
	filter_set.filters[0] = std::move(filter);

	unordered_map<idx_t, idx_t> filter_to_col;
	filter_to_col[0] = 0;
	vector<string> column_names {"a"};

	n6k_filter_json::FilterJsonOptions options;
	options.allow_nested = allow_nested;
	options.include_value_type_tags = tag_values;
	n6k_filter_json::FilterSerializeResult result;

	SelftestRow row;
	row.name = name;
	row.json = n6k_filter_json::SerializeFilters(filter_set, filter_to_col, column_names, options, result);
	row.exact = result.all_exact;
	return row;
}

vector<SelftestRow> BuildRows() {
	vector<SelftestRow> rows;

	rows.push_back(Run("gt", Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)), true));

	rows.push_back(Run("is_null", make_uniq<IsNullFilter>(), true));

	{
		vector<Value> values {Value::INTEGER(1), Value::INTEGER(2)};
		rows.push_back(Run("in", make_uniq<InFilter>(std::move(values)), true));
	}

	// OR must stay a disjunction: flattening its children into the AND-joined top level reads as
	// `a > 5 AND a < 2`, which matches nothing. DuckDB pushes disjunctions wrapped in an
	// OptionalFilter and keeps a FILTER above the scan, so the old flattening cost rows over the
	// wire rather than correctness -- but the wire format has to mean what it says regardless.
	{
		vector<unique_ptr<TableFilter>> children;
		children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		children.push_back(Cmp(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(2)));
		rows.push_back(Run("or", Conjunction(false, std::move(children)), true));
	}

	// A multi-clause branch inside a disjunction needs its own "and" group; splicing its clauses
	// into the "or" list would read as two independent branches.
	{
		vector<unique_ptr<TableFilter>> and_children;
		and_children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		and_children.push_back(Cmp(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(10)));
		vector<unique_ptr<TableFilter>> or_children;
		or_children.push_back(Conjunction(true, std::move(and_children)));
		or_children.push_back(Cmp(ExpressionType::COMPARE_EQUAL, Value::INTEGER(0)));
		rows.push_back(Run("and_in_or", Conjunction(false, std::move(or_children)), true));
	}

	// The flat pyarrow dialect has no disjunction, so the whole set must be refused rather than
	// silently AND-ed.
	{
		vector<unique_ptr<TableFilter>> children;
		children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		children.push_back(Cmp(ExpressionType::COMPARE_LESSTHAN, Value::INTEGER(2)));
		rows.push_back(Run("or_flat_dialect", Conjunction(false, std::move(children)), false));
	}

	rows.push_back(Run("optional_supported",
	                   make_uniq<OptionalFilter>(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5))), true));

	// An optional filter is a hint enforced elsewhere, so dropping an unrenderable one is safe and
	// leaves the set exact.
	rows.push_back(Run("optional_unrenderable", make_uniq<OptionalFilter>(Unrenderable()), true));

	rows.push_back(Run("unrenderable", Unrenderable(), false));

	// Value type tags — the provider dialect only. A clause stays three elements unless its value
	// crossed as text and has to be rebuilt on the far side, so the tag's PRESENCE is the signal.
	rows.push_back(Run("tag_absent_for_int", Cmp(ExpressionType::COMPARE_EQUAL, Value::INTEGER(5)), false, true));
	rows.push_back(Run("tag_absent_for_varchar", Cmp(ExpressionType::COMPARE_EQUAL, Value("abc")), false, true));
	rows.push_back(
	    Run("tag_date", Cmp(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::DATE(date_t(20214))), false, true));
	rows.push_back(
	    Run("tag_decimal", Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::DECIMAL(int64_t(150), 10, 2)), false, true));
	rows.push_back(
	    Run("tag_hugeint", Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::HUGEINT(hugeint_t(12345))), false, true));
	rows.push_back(Run("tag_opaque", Cmp(ExpressionType::COMPARE_EQUAL, Value::BLOB("ab")), false, true));
	// Untagged by default, so the wire dialect is byte-identical to what it always emitted.
	rows.push_back(Run("tag_off_by_default", Cmp(ExpressionType::COMPARE_EQUAL, Value::DATE(date_t(20214))), true));
	{
		vector<Value> values {Value::DATE(date_t(20214)), Value::DATE(date_t(20234))};
		rows.push_back(Run("tag_in_list", make_uniq<InFilter>(std::move(values)), false, true));
	}

	// The critical AND case: emitting only `a > 5` would be a weaker predicate, and nothing
	// re-applies the dropped half locally.
	{
		vector<unique_ptr<TableFilter>> children;
		children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		children.push_back(Unrenderable());
		rows.push_back(Run("and_with_unrenderable", Conjunction(true, std::move(children)), true));
	}

	{
		vector<unique_ptr<TableFilter>> children;
		children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		children.push_back(Unrenderable());
		rows.push_back(Run("or_with_unrenderable", Conjunction(false, std::move(children)), true));
	}

	// Dropping a branch widens a disjunction, so an optional branch is not good enough either.
	{
		vector<unique_ptr<TableFilter>> children;
		children.push_back(Cmp(ExpressionType::COMPARE_GREATERTHAN, Value::INTEGER(5)));
		children.push_back(make_uniq<OptionalFilter>(Unrenderable()));
		rows.push_back(Run("or_with_optional_branch", Conjunction(false, std::move(children)), true));
	}

	return rows;
}

struct N6kFilterJsonSelftestBind : public TableFunctionData {};

struct N6kFilterJsonSelftestState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"case_name", "json", "exact"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	return make_uniq<N6kFilterJsonSelftestBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kFilterJsonSelftestState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kFilterJsonSelftestState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	auto rows = BuildRows();
	for (idx_t i = 0; i < rows.size(); i++) {
		output.SetValue(0, i, Value(rows[i].name));
		output.SetValue(1, i, Value(rows[i].json));
		output.SetValue(2, i, Value::BOOLEAN(rows[i].exact));
	}
	output.SetCardinality(rows.size());
}

} // namespace

void RegisterN6kTestingFilterJsonFidelity(ExtensionLoader &loader) {
	TableFunction fn("n6k_testing_filter_json_fidelity", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
