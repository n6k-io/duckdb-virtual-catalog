#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

//! Binds `SELECT * FROM catalog.schema.table WHERE predicate` and runs only DuckDB's expression rewriter over
//! it; the statistics passes are skipped because they would make the answer depend on the rows. True only when
//! every filter folds to a constant true and the plan is nothing but a filtered scan, so a subquery is false.
//! Needs an active transaction on `context`.
bool PredicateFoldsToTrue(ClientContext &context, const string &catalog, const string &schema, const string &table,
                          const ParsedExpression &predicate);

} // namespace duckdb
