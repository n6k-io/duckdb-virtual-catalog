#pragma once

#include "catch.hpp"
#include "memory_source/memory_source.hpp"
#include "internal/seam.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

namespace duckdb {

inline string DSLExpressionText(const Expression &expr) {
	auto text = expr.ToString();
	if (text.size() < 2 || text.front() != '(' || text.back() != ')') {
		return text;
	}
	idx_t depth = 0;
	for (idx_t i = 0; i < text.size(); i++) {
		if (text[i] == '(') {
			depth++;
		} else if (text[i] == ')') {
			depth--;
			if (depth == 0 && i + 1 < text.size()) {
				return text;
			}
		}
	}
	return text.substr(1, text.size() - 2);
}

inline string DSLLimitText(const BoundLimitNode &node) {
	switch (node.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		return to_string(node.GetConstantValue());
	case LimitNodeType::CONSTANT_PERCENTAGE:
		return to_string(node.GetConstantPercentage()) + "%";
	case LimitNodeType::EXPRESSION_VALUE:
		return DSLExpressionText(node.GetValueExpression());
	default:
		return "";
	}
}

inline string DSLLabel(LogicalOperator &op, bool in_fragment) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return "proj";
	case LogicalOperatorType::LOGICAL_FILTER:
		return "filter";
	case LogicalOperatorType::LOGICAL_LIMIT:
		return "limit";
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return "distinct";
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return "sample";
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return "agg";
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return "order";
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return "join";
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return "cross";
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return "rows";
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return in_fragment ? "scan" : "local";
	case LogicalOperatorType::LOGICAL_GET:
		if (op.Cast<LogicalGet>().function.name == CROSSING_SEAM_FUNCTION) {
			return "seam";
		}
		return in_fragment ? "scan" : "local";
	default:
		return StringUtil::Lower(LogicalOperatorToString(op.type));
	}
}

inline string DSLBraces(LogicalOperator &op) {
	vector<string> parts;
	if (op.type == LogicalOperatorType::LOGICAL_DISTINCT) {
		for (auto &target : op.Cast<LogicalDistinct>().distinct_targets) {
			parts.push_back(DSLExpressionText(*target));
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		for (auto &order : op.Cast<LogicalOrder>().orders) {
			parts.push_back(DSLExpressionText(*order.expression));
		}
	}
	for (auto &expr : op.expressions) {
		parts.push_back(DSLExpressionText(*expr));
	}
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op.Cast<LogicalLimit>();
		auto rows = DSLLimitText(limit.limit_val);
		auto offset = DSLLimitText(limit.offset_val);
		if (!rows.empty()) {
			parts.push_back(rows);
		}
		if (!offset.empty()) {
			parts.push_back("offset " + offset);
		}
	}
	auto text = StringUtil::Join(parts, ", ");
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		vector<string> groups;
		for (auto &group : op.Cast<LogicalAggregate>().groups) {
			groups.push_back(DSLExpressionText(*group));
		}
		if (!groups.empty()) {
			text += (text.empty() ? "" : " ") + string("by ") + StringUtil::Join(groups, ", ");
		}
	}
	return text.empty() ? "" : "{" + text + "}";
}

inline string DSLRender(LogicalOperator &op, bool in_fragment) {
	if (auto fragment = CrossingReadFragmentOf(op)) {
		string inside = fragment->plan ? DSLRender(*fragment->plan, true) : string();
		return "crossing[" + inside + "]";
	}
	if (auto write = CrossingWriteFragmentOf(op)) {
		string inside = write->plan ? DSLRender(*write->plan, true) : string();
		return "crossing[" + inside + "]";
	}

	auto text = DSLLabel(op, in_fragment) + DSLBraces(op);
	if (op.children.empty()) {
		return text;
	}
	if (op.children.size() == 1) {
		return text + " | " + DSLRender(*op.children[0], in_fragment);
	}
	vector<string> rendered;
	for (auto &child : op.children) {
		rendered.push_back(DSLRender(*child, in_fragment));
	}
	return text + "(" + StringUtil::Join(rendered, ", ") + ")";
}

inline string PlanToTestDSL(LogicalOperator &op) {
	return DSLRender(op, false);
}

inline string PlanToTestDSL(const unique_ptr<LogicalOperator> &op) {
	return DSLRender(*op, false);
}

inline string FragmentToTestDSL(const CrossingFragment &fragment) {
	return fragment.plan ? DSLRender(*fragment.plan, true) : string();
}

} // namespace duckdb

#define REQUIRE_PLAN(plan, shape)         REQUIRE(PlanToTestDSL(plan) == string(shape))
#define REQUIRE_FRAGMENT(fragment, shape) REQUIRE(FragmentToTestDSL(fragment) == string(shape))
