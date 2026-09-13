#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "internal/source.hpp"

namespace duckdb {

//! One operator type's answer, for that node alone. Whether the subtree beneath it also crosses is
//! the labelling's question.
class CrossingRule {
public:
	virtual ~CrossingRule() {
	}

	virtual bool CanCross(LogicalOperator &op, CrossingSource &source) const = 0;
};

//! An operator with no rule does not cross. Silence is a refusal, never an oversight.
class CrossingRules {
public:
	static const CrossingRules &Get();

	optional_ptr<const CrossingRule> RuleFor(LogicalOperatorType type) const;

private:
	CrossingRules();

	void Register(LogicalOperatorType type, unique_ptr<CrossingRule> rule);

	struct Entry {
		LogicalOperatorType type;
		unique_ptr<CrossingRule> rule;
	};

	vector<Entry> entries;
};

bool IsMaterialisedRows(LogicalOperatorType type);

} // namespace duckdb
