#include "internal/rules.hpp"

#include "internal/source_evaluation.hpp"

#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

namespace {

//! The common answer: the node crosses when the source can compute everything it holds.
class EvaluableRule : public CrossingRule {
public:
	bool CanCross(LogicalOperator &op, CrossingSource &source) const override {
		return CanEvaluateAll(op, source);
	}
};

//! A limit's bound is not in `expressions`, so it is checked here rather than by CanEvaluateAll.
class LimitRule : public EvaluableRule {
public:
	bool CanCross(LogicalOperator &op, CrossingSource &source) const override {
		auto &limit = op.Cast<LogicalLimit>();
		if (!BoundCrosses(limit.limit_val) || !BoundCrosses(limit.offset_val)) {
			return false;
		}
		return EvaluableRule::CanCross(op, source);
	}

private:
	//! A percentage is of the source's row count, which is not the count the target would have taken.
	static bool BoundCrosses(const BoundLimitNode &node) {
		switch (node.Type()) {
		case LimitNodeType::UNSET:
		case LimitNodeType::CONSTANT_VALUE:
			return true;
		case LimitNodeType::EXPRESSION_VALUE:
			return IsColumnFreeExpression(node.GetValueExpression());
		default:
			return false;
		}
	}
};

class OrderRule : public EvaluableRule {
public:
	bool CanCross(LogicalOperator &op, CrossingSource &source) const override {
		for (auto &order : OrdersOf(op)) {
			if (!order.expression || !CanEvaluate(*order.expression, source)) {
				return false;
			}
		}
		return EvaluableRule::CanCross(op, source);
	}

private:
	static const vector<BoundOrderByNode> &OrdersOf(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
			return op.Cast<LogicalTopN>().orders;
		}
		return op.Cast<LogicalOrder>().orders;
	}
};

class NeverRule : public CrossingRule {
public:
	bool CanCross(LogicalOperator &op, CrossingSource &source) const override {
		return false;
	}
};

} // namespace

CrossingRules::CrossingRules() {
	// Reshaping the child's output is no obstacle: the scan above a crossed subtree is rebuilt from
	// what that subtree emits, whatever shape it is.
	Register(LogicalOperatorType::LOGICAL_FILTER, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_PROJECTION, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_DISTINCT, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_SAMPLE, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_WINDOW, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_UNNEST, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_PIVOT, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_LIMIT, make_uniq<LimitRule>());

	// Two branches are no obstacle either: the labelling asks each separately, and they cross
	// together only if they name one source.
	Register(LogicalOperatorType::LOGICAL_JOIN, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_COMPARISON_JOIN, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_ANY_JOIN, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_ASOF_JOIN, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_CROSS_PRODUCT, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_UNION, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_EXCEPT, make_uniq<EvaluableRule>());
	Register(LogicalOperatorType::LOGICAL_INTERSECT, make_uniq<EvaluableRule>());

	Register(LogicalOperatorType::LOGICAL_ORDER_BY, make_uniq<OrderRule>());
	Register(LogicalOperatorType::LOGICAL_TOP_N, make_uniq<OrderRule>());

	for (auto type : {LogicalOperatorType::LOGICAL_INSERT,
	                  LogicalOperatorType::LOGICAL_DELETE,
	                  LogicalOperatorType::LOGICAL_UPDATE,
	                  LogicalOperatorType::LOGICAL_MERGE_INTO,
	                  LogicalOperatorType::LOGICAL_COPY_TO_FILE,
	                  LogicalOperatorType::LOGICAL_COPY_DATABASE,
	                  LogicalOperatorType::LOGICAL_EXPORT,
	                  LogicalOperatorType::LOGICAL_CREATE_TABLE,
	                  LogicalOperatorType::LOGICAL_CREATE_INDEX,
	                  LogicalOperatorType::LOGICAL_CREATE_SEQUENCE,
	                  LogicalOperatorType::LOGICAL_CREATE_VIEW,
	                  LogicalOperatorType::LOGICAL_CREATE_SCHEMA,
	                  LogicalOperatorType::LOGICAL_CREATE_MACRO,
	                  LogicalOperatorType::LOGICAL_CREATE_TYPE,
	                  LogicalOperatorType::LOGICAL_ALTER,
	                  LogicalOperatorType::LOGICAL_DROP,
	                  LogicalOperatorType::LOGICAL_VACUUM,
	                  LogicalOperatorType::LOGICAL_ATTACH,
	                  LogicalOperatorType::LOGICAL_DETACH,
	                  LogicalOperatorType::LOGICAL_LOAD,
	                  LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS,
	                  LogicalOperatorType::LOGICAL_SET,
	                  LogicalOperatorType::LOGICAL_RESET,
	                  LogicalOperatorType::LOGICAL_PRAGMA,
	                  LogicalOperatorType::LOGICAL_TRANSACTION,
	                  LogicalOperatorType::LOGICAL_PREPARE,
	                  LogicalOperatorType::LOGICAL_EXECUTE,
	                  LogicalOperatorType::LOGICAL_CREATE_SECRET,
	                  LogicalOperatorType::LOGICAL_EXPLAIN,
	                  LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR}) {
		Register(type, make_uniq<NeverRule>());
	}
}

void CrossingRules::Register(LogicalOperatorType type, unique_ptr<CrossingRule> rule) {
	Entry entry;
	entry.type = type;
	entry.rule = std::move(rule);
	entries.push_back(std::move(entry));
}

optional_ptr<const CrossingRule> CrossingRules::RuleFor(LogicalOperatorType type) const {
	for (auto &entry : entries) {
		if (entry.type == type) {
			return entry.rule.get();
		}
	}
	return nullptr;
}

const CrossingRules &CrossingRules::Get() {
	static CrossingRules rules;
	return rules;
}

bool IsMaterialisedRows(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_CHUNK_GET || type == LogicalOperatorType::LOGICAL_EXPRESSION_GET;
}

} // namespace duckdb
