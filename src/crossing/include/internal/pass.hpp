#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Moves everything that can cross to the source: every subtree that runs wholly on one source folds
//! into a crossing scan.
//!
//! Wire into the optimizer's pre hook. Running before duckdb's optimizers means the plan is still
//! one node per operation, with nothing folded into a scan and nothing yet moved for the target's
//! benefit: what crosses is optimized by whoever runs it, what stays behind by duckdb afterwards.
void MoveCrossableWorkIntoFragments(unique_ptr<LogicalOperator> &plan);

//! Wire into the optimizer's post hook: it runs after duckdb has dropped the columns nobody reads.
void NarrowFragmentsToTheirScans(unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
