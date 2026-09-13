#pragma once

#include "catch.hpp"
#include "internal/source.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <map>

namespace duckdb {

constexpr idx_t MEMORY_FLOOR_INDEX = 0;
constexpr idx_t MEMORY_CROSSING_INDEX = 1;
constexpr idx_t MEMORY_GET_INDEX = 2;

inline string FunctionNameOf(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return expr.Cast<BoundFunctionExpression>().function.name;
	case ExpressionClass::BOUND_AGGREGATE:
		return expr.Cast<BoundAggregateExpression>().function.name;
	case ExpressionClass::BOUND_WINDOW: {
		auto &window = expr.Cast<BoundWindowExpression>();
		if (window.aggregate) {
			return window.aggregate->name;
		}
		return StringUtil::Lower(ExpressionTypeToString(expr.GetExpressionType()));
	}
	default:
		return string();
	}
}

inline unique_ptr<Expression> BoundAmt() {
	return make_uniq<BoundColumnRefExpression>("amt", LogicalType::INTEGER, ColumnBinding(MEMORY_GET_INDEX, 1));
}

inline unique_ptr<Expression> Int(int32_t value) {
	return make_uniq<BoundConstantExpression>(Value::INTEGER(value));
}

inline unique_ptr<Expression> Call(const string &name, FunctionStability stability = FunctionStability::CONSISTENT) {
	vector<unique_ptr<Expression>> args;
	args.push_back(BoundAmt());
	ScalarFunction function(name, {LogicalType::INTEGER}, LogicalType::INTEGER, nullptr);
	function.stability = stability;
	return make_uniq<BoundFunctionExpression>(LogicalType::INTEGER, function, std::move(args), nullptr);
}

class MemorySource : public CrossingSource {
public:
	explicit MemorySource(case_insensitive_set_t known_p = case_insensitive_set_t()) : known(std::move(known_p)) {
	}

	vector<string> Tables(const string &schema) override {
		return {};
	}
	CrossingTable Describe(const string &schema, const string &name) override {
		throw InternalException("memory source: nothing to describe");
	}
	unique_ptr<LogicalOperator> Plan(CrossingPlanRequest &request) override {
		throw InternalException("memory source: nothing to plan");
	}
	CrossingVerdict AcceptsCall(const Expression &expr) override {
		auto name = FunctionNameOf(expr);
		if (known.find(name) != known.end()) {
			return CrossingVerdict::Yes();
		}
		return CrossingVerdict::No("the memory source has no " + name);
	}
	CrossingVerdict AcceptsType(const LogicalType &type) override {
		return CrossingVerdict::Yes();
	}
	unique_ptr<CrossingReader> Read(CrossingTransaction &transaction, const CrossingReadQuery &query) override {
		throw InternalException("memory source: nothing to read");
	}

private:
	case_insensitive_set_t known;
};

inline MemorySource &MemorySourceFor(const string &id, const case_insensitive_set_t &known) {
	static std::map<string, unique_ptr<MemorySource>> sources;
	string key = id;
	for (auto &name : known) {
		key += "|" + name;
	}
	auto &slot = sources[key];
	if (!slot) {
		slot = make_uniq<MemorySource>(known);
	}
	return *slot;
}

struct MemorySourceBindData : public TableFunctionData, public CrossingReadCarrier {
	shared_ptr<CrossingFragment> fragment;
	optional_ptr<CrossingSource> source;

	optional_ptr<CrossingFragment> GetReadFragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return *source.get_mutable();
	}
};

inline shared_ptr<CrossingFragment> MemoryFragment() {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = {"id", "amt"};
	fragment->column_types = {LogicalType::INTEGER, LogicalType::INTEGER};
	fragment->floor_bindings = {ColumnBinding(MEMORY_FLOOR_INDEX, 0), ColumnBinding(MEMORY_FLOOR_INDEX, 1)};
	fragment->table_index = MEMORY_CROSSING_INDEX;

	auto floor = make_uniq<LogicalDummyScan>(MEMORY_FLOOR_INDEX);
	fragment->SealFloor(*floor);
	fragment->floor = std::move(floor);
	return fragment;
}

inline unique_ptr<LogicalOperator> MemorySourceScan(case_insensitive_set_t known = case_insensitive_set_t(),
                                                    const string &id = "memory") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});

	auto bind_data = make_uniq<MemorySourceBindData>();
	bind_data->fragment = fragment;
	bind_data->source = &MemorySourceFor(id, known);

	auto types = fragment->column_types;
	auto names = fragment->column_names;
	TableFunction function("memory_source_scan", {}, nullptr);
	auto get =
	    make_uniq<LogicalGet>(MEMORY_GET_INDEX, function, std::move(bind_data), std::move(types), std::move(names));
	get->SetColumnIds({ColumnIndex(0), ColumnIndex(1)});
	return std::move(get);
}

} // namespace duckdb
