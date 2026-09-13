#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

//! Per node, the source its whole subtree could run on. A node is absent when its subtree spans two
//! sources, holds something that cannot cross, or reaches a leaf that is not a crossing scan.
//! A subtree of nothing but rows the target holds is labelled with no source at all: it can run
//! wherever the subtree above it goes.
struct SubtreeLabel {
	CrossingSource *source = nullptr;
	bool runs_anywhere = false;
};

using SubtreeLabels = unordered_map<LogicalOperator *, SubtreeLabel>;

SubtreeLabels LabelSubtrees(LogicalOperator &plan);

SubtreeLabels LabelSubtreesUnder(LogicalOperator &feed, CrossingSource &source);

} // namespace duckdb
