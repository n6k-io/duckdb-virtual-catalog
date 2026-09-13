#pragma once

// A crossing scan the planner produced, rather than one built by hand. Every other test asserts
// against a hand-written shape; these assert against the shape duckdb actually builds.

#include "catch.hpp"
#include "memory_source/memory_source.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

namespace {

//! A bind callback is a plain function pointer, so what the source answers for is set here rather
//! than captured.
inline case_insensitive_set_t &BoundMemorySourceKnown() {
	static case_insensitive_set_t known;
	return known;
}

unique_ptr<FunctionData> BoundMemorySourceBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});

	return_types = fragment->column_types;
	names = fragment->column_names;

	auto bind_data = make_uniq<MemorySourceBindData>();
	bind_data->fragment = fragment;
	bind_data->source = &MemorySourceFor("memory", BoundMemorySourceKnown());
	return std::move(bind_data);
}

void BoundMemorySourceScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	output.SetCardinality(0);
}

} // namespace

struct Bound {
	DuckDB db;
	Connection con;

	explicit Bound(case_insensitive_set_t known = case_insensitive_set_t()) : db(nullptr), con(db) {
		BoundMemorySourceKnown() = std::move(known);
		TableFunction function("memory_source", {}, BoundMemorySourceScan, BoundMemorySourceBind);
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();

		auto created = con.Query("CREATE TABLE t(id INTEGER, amt INTEGER)");
		REQUIRE(!created->HasError());
	}

	//! The plan as the binder leaves it. Not ExtractPlan: that runs ColumnBindingResolver, which
	//! rewrites every BoundColumnRef into a BoundReference and leaves nothing the rules recognise.
	unique_ptr<LogicalOperator> Bind(const string &sql) {
		auto statements = con.context->ParseStatements(sql);
		REQUIRE(statements.size() == 1);

		con.BeginTransaction();
		Planner planner(*con.context);
		planner.CreatePlan(std::move(statements[0]));
		auto plan = std::move(planner.plan);
		con.Rollback();
		return plan;
	}
};

} // namespace duckdb
