#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

//! Hung off a scan's bind data, which is what makes an ordinary LogicalGet a crossing scan.
class CrossingReadCarrier {
public:
	virtual ~CrossingReadCarrier() {
	}

	virtual optional_ptr<CrossingFragment> GetReadFragment() = 0;

	virtual CrossingSource &Source() = 0;
};

//! Hung off a scan's bind data too: a write that runs wholly on the source is a scan of one count.
class CrossingWriteCarrier {
public:
	virtual ~CrossingWriteCarrier() {
	}

	virtual optional_ptr<CrossingFragment> GetWriteFragment() = 0;

	virtual CrossingSource &Source() = 0;
};

optional_ptr<CrossingFragment> CrossingReadFragmentOf(LogicalOperator &op);

optional_ptr<CrossingFragment> CrossingWriteFragmentOf(LogicalOperator &op);

optional_ptr<CrossingSource> CrossingSourceOf(LogicalOperator &op);

} // namespace duckdb
