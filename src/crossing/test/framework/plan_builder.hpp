#pragma once

#include "framework/plan_dsl.hpp"
#include "framework/stub_seam.hpp"
#include "internal/scan_columns.hpp"

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

constexpr idx_t BUILDER_LOCAL_INDEX = 14;
constexpr idx_t BUILDER_WINDOW_INDEX = 15;
constexpr idx_t BUILDER_UNNEST_INDEX = 16;
constexpr idx_t BUILDER_PROJECTION_INDEX = 20;

inline ColumnBinding UnresolvedBinding() {
	return ColumnBinding(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
}

inline unique_ptr<Expression> Col(const string &name) {
	return make_uniq<BoundColumnRefExpression>(name, LogicalType::SQLNULL, UnresolvedBinding());
}

inline unique_ptr<Expression> Bool(bool value) {
	return make_uniq<BoundConstantExpression>(Value::BOOLEAN(value));
}

inline unique_ptr<Expression> Gt(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	return make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(left), std::move(right));
}

inline unique_ptr<Expression> CaseWhen(unique_ptr<Expression> when, unique_ptr<Expression> then,
                                       unique_ptr<Expression> otherwise) {
	auto result = make_uniq<BoundCaseExpression>(then->return_type);
	result->case_checks.emplace_back();
	result->case_checks[0].when_expr = std::move(when);
	result->case_checks[0].then_expr = std::move(then);
	result->else_expr = std::move(otherwise);
	return std::move(result);
}

inline unique_ptr<Expression> UnknownFn(const string &name, unique_ptr<Expression> argument) {
	ScalarFunction function(name, {argument->return_type}, LogicalType::BOOLEAN, nullptr);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(argument));
	return make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, std::move(function), std::move(arguments), nullptr);
}

struct BuilderColumn {
	string name;
	ColumnBinding binding;
	LogicalType type;
};

inline void BuilderColumnsOf(LogicalOperator &op, vector<BuilderColumn> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto bindings = get.GetColumnBindings();
		auto &ids = get.GetColumnIds();
		for (idx_t i = 0; i < bindings.size() && i < ids.size(); i++) {
			auto id = ids[i].GetPrimaryIndex();
			out.push_back({get.names[id], bindings[i], get.returned_types[id]});
		}
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
		auto bindings = op.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size() && i < op.types.size(); i++) {
			out.push_back({"c" + to_string(i), bindings[i], op.types[i]});
		}
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		auto bindings = projection.GetColumnBindings();
		for (idx_t i = 0; i < projection.expressions.size() && i < bindings.size(); i++) {
			out.push_back({projection.expressions[i]->GetName(), bindings[i], projection.expressions[i]->return_type});
		}
		return;
	}
	for (auto &child : op.children) {
		BuilderColumnsOf(*child, out);
	}
}

inline void BuilderResolveExpression(unique_ptr<Expression> &expr, const vector<BuilderColumn> &columns,
                                     const string &where) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index != DConstants::INVALID_INDEX) {
			return;
		}
		for (auto &column : columns) {
			if (!StringUtil::CIEquals(column.name, ref.GetName())) {
				continue;
			}
			ref.binding = column.binding;
			ref.return_type = column.type;
			return;
		}
		vector<string> available;
		for (auto &column : columns) {
			available.push_back(column.name);
		}
		FAIL("plan_builder: no column named '" << ref.GetName() << "' below " << where
		                                       << "; below it are: " << StringUtil::Join(available, ", "));
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { BuilderResolveExpression(child, columns, where); });
}

inline void BuilderEachExpression(LogicalOperator &op, const std::function<void(unique_ptr<Expression> &)> &callback) {
	for (auto &expr : op.expressions) {
		callback(expr);
	}
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		for (auto &order : op.Cast<LogicalOrder>().orders) {
			callback(order.expression);
		}
	}
}

inline void BuilderResolve(LogicalOperator &op) {
	for (auto &child : op.children) {
		BuilderResolve(*child);
	}
	vector<BuilderColumn> columns;
	for (auto &child : op.children) {
		BuilderColumnsOf(*child, columns);
	}
	auto where = StringUtil::Lower(LogicalOperatorToString(op.type));
	BuilderEachExpression(op, [&](unique_ptr<Expression> &expr) { BuilderResolveExpression(expr, columns, where); });
	op.ResolveOperatorTypes();
}

inline void BuilderNumberProjections(LogicalOperator &op, idx_t &next) {
	for (auto &child : op.children) {
		BuilderNumberProjections(*child, next);
	}
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = op.Cast<LogicalProjection>();
	if (projection.table_index == DConstants::INVALID_INDEX) {
		projection.table_index = next++;
	}
}

inline unique_ptr<LogicalOperator> Chain(unique_ptr<LogicalOperator> op) {
	return op;
}

template <class... Rest>
unique_ptr<LogicalOperator> Chain(unique_ptr<LogicalOperator> op, Rest &&...rest) {
	op->children.push_back(Chain(std::forward<Rest>(rest)...));
	return op;
}

template <class... Nodes>
unique_ptr<LogicalOperator> Build(Nodes &&...nodes) {
	auto plan = Chain(std::forward<Nodes>(nodes)...);
	idx_t next = BUILDER_PROJECTION_INDEX;
	BuilderNumberProjections(*plan, next);
	BuilderResolve(*plan);
	return plan;
}

inline unique_ptr<LogicalOperator> Scan(case_insensitive_set_t known = case_insensitive_set_t()) {
	return MemorySourceScan(std::move(known));
}

inline unique_ptr<LogicalOperator> ScanOf(const string &source_id) {
	return MemorySourceScan({}, source_id);
}

inline unique_ptr<LogicalOperator> Local() {
	return make_uniq<LogicalDummyScan>(BUILDER_LOCAL_INDEX);
}

inline void Align(unique_ptr<LogicalOperator> &scan) {
	AlignFragmentToScan(scan->Cast<LogicalGet>(), *CrossingReadFragmentOf(*scan));
}

inline void Narrow(unique_ptr<LogicalOperator> &scan) {
	NarrowFragmentAndScanToRequestedColumns(scan->Cast<LogicalGet>(), *CrossingReadFragmentOf(*scan));
}

inline void BuilderPushOne(vector<unique_ptr<Expression>> &into, unique_ptr<Expression> expr) {
	into.push_back(std::move(expr));
}

inline void BuilderPushOne(vector<unique_ptr<Expression>> &into, const char *name) {
	into.push_back(Col(name));
}

inline void BuilderPush(vector<unique_ptr<Expression>> &into) {
}

template <class First, class... Rest>
void BuilderPush(vector<unique_ptr<Expression>> &into, First &&first, Rest &&...rest) {
	BuilderPushOne(into, std::forward<First>(first));
	BuilderPush(into, std::forward<Rest>(rest)...);
}

template <class... Exprs>
unique_ptr<LogicalOperator> Filter(Exprs &&...exprs) {
	auto filter = make_uniq<LogicalFilter>();
	BuilderPush(filter->expressions, std::forward<Exprs>(exprs)...);
	return std::move(filter);
}

template <class... Exprs>
unique_ptr<LogicalOperator> Proj(Exprs &&...exprs) {
	vector<unique_ptr<Expression>> expressions;
	BuilderPush(expressions, std::forward<Exprs>(exprs)...);
	return make_uniq<LogicalProjection>(DConstants::INVALID_INDEX, std::move(expressions));
}

inline unique_ptr<LogicalOperator> Limit(int64_t rows) {
	return make_uniq<LogicalLimit>(BoundLimitNode::ConstantValue(rows), BoundLimitNode());
}

template <class... Exprs>
unique_ptr<LogicalOperator> Order(Exprs &&...exprs) {
	vector<unique_ptr<Expression>> keys;
	BuilderPush(keys, std::forward<Exprs>(exprs)...);
	vector<BoundOrderByNode> orders;
	for (auto &key : keys) {
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(key));
	}
	return make_uniq<LogicalOrder>(std::move(orders));
}

inline unique_ptr<LogicalOperator> RowNumberOver(unique_ptr<Expression> order_key) {
	auto window =
	    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
	window->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(order_key));
	window->start = WindowBoundary::UNBOUNDED_PRECEDING;
	window->end = WindowBoundary::CURRENT_ROW_RANGE;
	auto op = make_uniq<LogicalWindow>(BUILDER_WINDOW_INDEX);
	op->expressions.push_back(std::move(window));
	return std::move(op);
}

inline unique_ptr<LogicalOperator> Unnest(unique_ptr<Expression> list) {
	auto unnest = make_uniq<BoundUnnestExpression>(LogicalType::INTEGER);
	unnest->child = std::move(list);
	auto op = make_uniq<LogicalUnnest>(BUILDER_UNNEST_INDEX);
	op->expressions.push_back(std::move(unnest));
	return std::move(op);
}

inline unique_ptr<LogicalOperator> Join(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right) {
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	join->children.push_back(std::move(left));
	join->children.push_back(std::move(right));
	return std::move(join);
}

class DSLParser {
public:
	explicit DSLParser(const string &text_p) : text(text_p) {
	}

	unique_ptr<LogicalOperator> Plan() {
		auto plan = ParseChain();
		SkipSpace();
		if (at < text.size()) {
			Fail("unexpected '" + text.substr(at) + "'");
		}
		return plan;
	}

private:
	const string &text;
	idx_t at = 0;

	void Fail(const string &why) {
		FAIL("plan_dsl: " << why << " at " << at << " in \"" << text << "\"");
	}

	void SkipSpace() {
		while (at < text.size() && isspace(text[at])) {
			at++;
		}
	}

	bool Peek(char c) {
		SkipSpace();
		return at < text.size() && text[at] == c;
	}

	bool Take(char c) {
		if (!Peek(c)) {
			return false;
		}
		at++;
		return true;
	}

	void Expect(char c) {
		if (!Take(c)) {
			Fail(string("expected '") + c + "'");
		}
	}

	string Word() {
		SkipSpace();
		auto start = at;
		while (at < text.size() && (isalnum(text[at]) || text[at] == '_')) {
			at++;
		}
		if (at == start) {
			Fail("expected a name");
		}
		return text.substr(start, at - start);
	}

	unique_ptr<LogicalOperator> ParseChain() {
		auto node = ParseNode();
		if (Take('|')) {
			node->children.push_back(ParseChain());
		}
		return node;
	}

	unique_ptr<LogicalOperator> ParseNode() {
		auto name = Word();
		if (name == "crossing") {
			return ParseCrossing();
		}
		vector<unique_ptr<Expression>> args;
		if (Take('{')) {
			while (!Peek('}')) {
				args.push_back(ParseExpression());
				if (!Take(',')) {
					break;
				}
			}
			Expect('}');
		}
		if (name == "filter") {
			auto filter = make_uniq<LogicalFilter>();
			filter->expressions = std::move(args);
			return std::move(filter);
		}
		if (name == "proj") {
			return make_uniq<LogicalProjection>(DConstants::INVALID_INDEX, std::move(args));
		}
		if (name == "limit") {
			if (args.size() != 1 || args[0]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				Fail("limit takes one integer");
			}
			return Limit(args[0]->Cast<BoundConstantExpression>().value.GetValue<int64_t>());
		}
		if (name == "order") {
			vector<BoundOrderByNode> orders;
			for (auto &key : args) {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, std::move(key));
			}
			return make_uniq<LogicalOrder>(std::move(orders));
		}
		if (name == "join") {
			Expect('(');
			auto left = ParseChain();
			Expect(',');
			auto right = ParseChain();
			Expect(')');
			return Join(std::move(left), std::move(right));
		}
		if (name == "local") {
			return Local();
		}
		if (name == "rows") {
			return Rows();
		}
		Fail("unknown node '" + name + "'");
		return nullptr;
	}

	unique_ptr<LogicalOperator> ParseCrossing() {
		Expect('[');
		auto head = Word();
		if (head != "proj") {
			Fail("a crossing region starts with proj");
		}
		Expect('{');
		vector<string> columns;
		while (!Peek('}')) {
			columns.push_back(Word());
			if (!Take(',')) {
				break;
			}
		}
		Expect('}');
		Expect('|');
		auto leaf = Word();
		Expect(']');
		if (leaf == "scan") {
			if (columns != vector<string> {"id", "amt"}) {
				Fail("a crossing scan projects id, amt; narrow it by hand");
			}
			return Scan();
		}
		Fail("a crossing region ends in scan");
		return nullptr;
	}

	unique_ptr<Expression> ParseExpression() {
		auto left = ParseAtom();
		SkipSpace();
		if (at < text.size() && text[at] == '>') {
			at++;
			return Gt(std::move(left), ParseAtom());
		}
		return left;
	}

	unique_ptr<Expression> ParseAtom() {
		SkipSpace();
		if (at < text.size() && isdigit(text[at])) {
			auto start = at;
			while (at < text.size() && isdigit(text[at])) {
				at++;
			}
			return Int(std::stoi(text.substr(start, at - start)));
		}
		auto name = Word();
		if (name == "true") {
			return Bool(true);
		}
		if (name == "false") {
			return Bool(false);
		}
		if (Take('(')) {
			auto argument = ParseExpression();
			Expect(')');
			return UnknownFn(name, std::move(argument));
		}
		return Col(name);
	}
};

inline unique_ptr<LogicalOperator> PlanFromDSL(const string &text) {
	DSLParser parser(text);
	auto plan = parser.Plan();
	idx_t next = BUILDER_PROJECTION_INDEX;
	BuilderNumberProjections(*plan, next);
	BuilderResolve(*plan);
	REQUIRE(PlanToTestDSL(plan) == text);
	return plan;
}

} // namespace duckdb
