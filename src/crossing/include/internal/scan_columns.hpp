#pragma once

#include "duckdb/planner/operator/logical_get.hpp"

#include "internal/fragment.hpp"

namespace duckdb {

//! Rebuilds the fragment's projection from the columns `get` is asked for, and gives the fragment the
//! scan's table index so a ColumnBinding means the same column on both sides.
//!
//! A scan may name columns the source does not have -- a virtual column, or one past the end of the
//! table. Those are dropped rather than refused: they are the target's business.
void AlignFragmentToScan(LogicalGet &get, CrossingFragment &fragment);

void NarrowFragmentAndScanToRequestedColumns(LogicalGet &get, CrossingFragment &fragment);

} // namespace duckdb
