#include "internal/filter_halves.hpp"

#include "internal/source.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

namespace {

//! Which source a filter is split against: the first crossing scan beneath it. A filter with none
//! below has nothing to cross into and is left whole.
optional_ptr<CrossingSource> SourceOfFirstScanBelow(LogicalOperator &op) {
	if (auto source = CrossingSourceOf(op)) {
		return source;
	}
	for (auto &child : op.children) {
		if (auto found = SourceOfFirstScanBelow(*child)) {
			return found;
		}
	}
	return nullptr;
}

void SplitOneFilter(unique_ptr<LogicalOperator> &node, optional_ptr<CrossingSource> against, bool evaluable_above) {
	auto &filter = node->Cast<LogicalFilter>();
	if (filter.expressions.size() < 2 || filter.children.size() != 1) {
		return;
	}
	auto source = against ? against : SourceOfFirstScanBelow(*filter.children[0]);
	if (!source) {
		return;
	}

	vector<unique_ptr<Expression>> evaluable;
	vector<unique_ptr<Expression>> kept;
	for (auto &expr : filter.expressions) {
		if (CanEvaluate(*expr, *source)) {
			evaluable.push_back(std::move(expr));
		} else {
			kept.push_back(std::move(expr));
		}
	}
	if (evaluable.empty() || kept.empty()) {
		filter.expressions.clear();
		for (auto &expr : evaluable) {
			filter.expressions.push_back(std::move(expr));
		}
		for (auto &expr : kept) {
			filter.expressions.push_back(std::move(expr));
		}
		return;
	}

	auto &upper_half = evaluable_above ? evaluable : kept;
	auto &lower_half = evaluable_above ? kept : evaluable;

	auto lower = make_uniq<LogicalFilter>();
	lower->expressions = std::move(lower_half);
	lower->children.push_back(std::move(filter.children[0]));
	lower->ResolveOperatorTypes();

	filter.expressions = std::move(upper_half);
	filter.children[0] = std::move(lower);
	filter.ResolveOperatorTypes();
}

//! Only a filter directly above another rejoins -- that is the shape the split leaves behind, and
//! anything standing between two filters may be reading what the lower one emits.
bool RejoinOneFilter(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.children.size() != 1) {
		return false;
	}
	auto &child = *op.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_FILTER) {
		return false;
	}
	if (!child.Cast<LogicalFilter>().projection_map.empty()) {
		return false;
	}

	auto &filter = op.Cast<LogicalFilter>();
	auto &below = child.Cast<LogicalFilter>();
	for (auto &expr : below.expressions) {
		filter.expressions.push_back(std::move(expr));
	}
	op.children[0] = std::move(below.children[0]);
	op.ResolveOperatorTypes();
	return true;
}

} // namespace

void SplitFiltersAtEvaluableHalf(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		SplitFiltersAtEvaluableHalf(child);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		SplitOneFilter(plan, nullptr, false);
	}
}

void SplitFiltersAgainst(unique_ptr<LogicalOperator> &plan, CrossingSource &source) {
	for (auto &child : plan->children) {
		SplitFiltersAgainst(child, source);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		SplitOneFilter(plan, &source, true);
	}
}

void RejoinAdjacentFiltersEverywhere(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		RejoinAdjacentFiltersEverywhere(child);
	}
	if (auto fragment = CrossingReadFragmentOf(*plan)) {
		if (fragment->plan) {
			RejoinAdjacentFiltersEverywhere(fragment->plan);
		}
	}
	while (RejoinOneFilter(*plan)) {
	}
}

} // namespace duckdb
