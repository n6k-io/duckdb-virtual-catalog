#include "internal/pass.hpp"

#include "internal/filter_halves.hpp"
#include "internal/fold.hpp"
#include "internal/labelling.hpp"
#include "internal/scan_columns.hpp"
#include "internal/source.hpp"

namespace duckdb {

namespace {

//! Top-down, so the first labelled node reached is the largest one: everything below it is labelled
//! too, and folding the biggest is what leaves the least behind.
void FoldEveryLabelledSubtree(unique_ptr<LogicalOperator> &node, const SubtreeLabels &sources) {
	auto label = sources.find(node.get());
	if (label != sources.end() && label->second.source && !SubtreeHoldsFrozenScan(*node)) {
		node = FoldSubtreeIntoItsFragment(std::move(node));
		return;
	}
	for (auto &child : node->children) {
		FoldEveryLabelledSubtree(child, sources);
	}
}

//! Before anything is asked about crossing, so a fragment describes the columns its scan wants now
//! rather than the ones it wanted when it was bound.
void AlignEveryFragmentToItsScan(LogicalOperator &node) {
	if (auto fragment = CrossingReadFragmentOf(node)) {
		if (!fragment->frozen) {
			AlignFragmentToScan(node.Cast<LogicalGet>(), *fragment);
		}
	}
	for (auto &child : node.children) {
		AlignEveryFragmentToItsScan(*child);
	}
}

bool PlanHoldsCrossing(LogicalOperator &op) {
	if (CrossingReadFragmentOf(op)) {
		return true;
	}
	for (auto &child : op.children) {
		if (PlanHoldsCrossing(*child)) {
			return true;
		}
	}
	return false;
}

} // namespace

void NarrowFragmentsToTheirScans(unique_ptr<LogicalOperator> &plan) {
	if (auto fragment = CrossingReadFragmentOf(*plan)) {
		if (!fragment->frozen) {
			NarrowFragmentAndScanToRequestedColumns(plan->Cast<LogicalGet>(), *fragment);
		}
	}
	for (auto &child : plan->children) {
		NarrowFragmentsToTheirScans(child);
	}
}

void MoveCrossableWorkIntoFragments(unique_ptr<LogicalOperator> &plan) {
	if (!PlanHoldsCrossing(*plan)) {
		return;
	}
	AlignEveryFragmentToItsScan(*plan);
	SplitFiltersAtEvaluableHalf(plan);
	auto sources = LabelSubtrees(*plan);
	FoldEveryLabelledSubtree(plan, sources);
	RejoinAdjacentFiltersEverywhere(plan);
}

} // namespace duckdb
