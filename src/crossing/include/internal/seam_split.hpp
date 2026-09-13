#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

struct SeamRow {
	unique_ptr<LogicalOperator> plan;
	optional_ptr<LogicalOperator> image;
};

SeamRow SeamRowOf(LogicalOperator &write, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed);

struct SeamStop {
	optional_ptr<LogicalOperator> node;
	string reason;
};

struct SeamSplit {
	unique_ptr<LogicalOperator> remainder;
	vector<LogicalType> boundary_types;
	string obstacle;
};

SeamSplit SplitFeedIntoSeam(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, CrossingSource &source,
                            const SeamStop &stop);

} // namespace duckdb
