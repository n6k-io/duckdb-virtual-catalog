#pragma once

// Statements are built here as parser nodes, never as text. A name stays a name and a value is
// either a bound parameter or a constant node, so neither can close a literal and carry on as
// syntax -- a column called `x FROM y` is a column, not a fragment of a query. This header is the
// one place those shapes are stated, and there is deliberately no escaping helper to reach for.

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

namespace duckdb {
namespace vcat {

//! `$index`. The identifier has to be the 1-based decimal string:
//! PreparedStatement::Execute(vector<Value> &) derives the key from a value's position in the vector.
inline unique_ptr<ParsedExpression> Param(idx_t index) {
	auto param = make_uniq<ParameterExpression>();
	param->identifier = to_string(index);
	return std::move(param);
}

//! Declare `$1..$count` on a hand-built statement. Not optional: ClientContext::PrepareInternal
//! copies named_param_map verbatim and infers nothing from the tree, and PreparedStatementData::Bind
//! then asserts its size against the bound parameter count.
inline void SetParamCount(SQLStatement &stmt, idx_t count) {
	for (idx_t i = 1; i <= count; i++) {
		stmt.named_param_map[to_string(i)] = i;
	}
}

//! `SELECT udf(<args>)`.
inline unique_ptr<SQLStatement> UdfCall(const string &udf_name, vector<unique_ptr<ParsedExpression>> args) {
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<FunctionExpression>(udf_name, std::move(args)));
	// Binder::BindNode dereferences from_table unconditionally; the parser puts this here too.
	node->from_table = make_uniq<EmptyTableRef>();
	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(node);
	return std::move(stmt);
}

//! `SELECT udf($1, .., $param_count)`, ready to prepare.
inline unique_ptr<SQLStatement> UdfCallWithParams(const string &udf_name, idx_t param_count) {
	vector<unique_ptr<ParsedExpression>> args;
	for (idx_t i = 1; i <= param_count; i++) {
		args.push_back(Param(i));
	}
	auto stmt = UdfCall(udf_name, std::move(args));
	SetParamCount(*stmt, param_count);
	return stmt;
}

//! `SELECT udf(<args>)` with the arguments inlined as constant nodes, for the paths that run the
//! statement once rather than preparing it.
inline unique_ptr<SQLStatement> UdfCallWithConstants(const string &udf_name, const vector<Value> &args) {
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &value : args) {
		children.push_back(make_uniq<ConstantExpression>(value));
	}
	return UdfCall(udf_name, std::move(children));
}

//! A three-part table name as a table reference, for a hand-built DML statement.
inline unique_ptr<TableRef> BaseTable(const string &catalog, const string &schema, const string &table) {
	auto ref = make_uniq<BaseTableRef>();
	ref->catalog_name = catalog;
	ref->schema_name = schema;
	ref->table_name = table;
	return std::move(ref);
}

} // namespace vcat
} // namespace duckdb
