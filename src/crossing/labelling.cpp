#include "internal/labelling.hpp"

#include "internal/rules.hpp"
#include "internal/source.hpp"

namespace duckdb {

namespace {

bool Unlabelled(const SubtreeLabel &label) {
	return !label.source && !label.runs_anywhere;
}

bool CrossesWholeTo(LogicalOperator &op, CrossingSource &source) {
	if (IsMaterialisedRows(op.type)) {
		return true;
	}
	auto rule = CrossingRules::Get().RuleFor(op.type);
	if (!rule || !rule->CanCross(op, source)) {
		return false;
	}
	for (auto &child : op.children) {
		if (!CrossesWholeTo(*child, source)) {
			return false;
		}
	}
	return true;
}

SubtreeLabel LabelSubtree(LogicalOperator &op, SubtreeLabels &out, optional_ptr<CrossingSource> fence_source) {
	if (CrossingReadFragmentOf(op)) {
		SubtreeLabel here;
		here.source = CrossingSourceOf(op).get();
		out[&op] = here;
		return here;
	}
	if (IsMaterialisedRows(op.type)) {
		SubtreeLabel here;
		here.runs_anywhere = true;
		out[&op] = here;
		return here;
	}

	// Every child is labelled before any of them is judged: a branch that disagrees stops this node
	// from crossing, but the branches themselves keep whatever answers they had.
	SubtreeLabel agreed;
	bool agrees = !op.children.empty();
	vector<reference<LogicalOperator>> unasked;
	for (auto &child : op.children) {
		auto child_label = LabelSubtree(*child, out, fence_source);
		if (Unlabelled(child_label)) {
			agrees = false;
			continue;
		}
		if (!child_label.source) {
			agreed.runs_anywhere = true;
			if (!fence_source) {
				unasked.emplace_back(*child);
			}
			continue;
		}
		if (agreed.source && agreed.source != child_label.source) {
			agrees = false;
			continue;
		}
		agreed.source = child_label.source;
	}
	if (!agrees) {
		return {};
	}
	if (agreed.source) {
		agreed.runs_anywhere = false;
		for (auto &child : unasked) {
			if (!CrossesWholeTo(child.get(), *agreed.source)) {
				return {};
			}
		}
	}

	auto asked = agreed.source ? agreed.source : fence_source.get();
	auto rule = CrossingRules::Get().RuleFor(op.type);
	if (!rule) {
		return {};
	}
	if (asked && !rule->CanCross(op, *asked)) {
		return {};
	}

	out[&op] = agreed;
	return agreed;
}

} // namespace

SubtreeLabels LabelSubtrees(LogicalOperator &plan) {
	SubtreeLabels out;
	LabelSubtree(plan, out, nullptr);
	return out;
}

SubtreeLabels LabelSubtreesUnder(LogicalOperator &feed, CrossingSource &source) {
	SubtreeLabels out;
	LabelSubtree(feed, out, &source);
	return out;
}

} // namespace duckdb
