#include "catch.hpp"
#include "predicate_fold.hpp"

#include "duckdb/parser/parser.hpp"

using namespace duckdb;

namespace {

bool FoldsToTrue(Connection &con, const string &text) {
	auto parsed = Parser::ParseExpressionList(text);
	REQUIRE(parsed.size() == 1);
	bool result = false;
	con.context->RunFunctionInTransaction(
	    [&]() { result = PredicateFoldsToTrue(*con.context, "memory", "main", "users", *parsed[0]); });
	return result;
}

void CreateUsers(Connection &con, const string &rows) {
	REQUIRE_FALSE(con.Query("CREATE TABLE users(id INTEGER, tenant INTEGER)")->HasError());
	if (!rows.empty()) {
		REQUIRE_FALSE(con.Query("INSERT INTO users VALUES " + rows)->HasError());
	}
}

} // namespace

TEST_CASE("predicate_fold: context-only branches fold to true", "[predicate_fold]") {
	DuckDB db(nullptr);
	Connection con(db);
	CreateUsers(con, "(1, 1), (2, 2), (3, 3)");

	CHECK(FoldsToTrue(con, "true"));
	CHECK(FoldsToTrue(con, "'admin' = 'admin'"));
	CHECK(FoldsToTrue(con, "'admin' = 'admin' OR tenant = 1"));
	CHECK(FoldsToTrue(con, "1 = 1 AND ('admin' = 'admin' OR tenant = 1)"));
	CHECK(FoldsToTrue(con, "NOT ('bob' = 'admin')"));
}

TEST_CASE("predicate_fold: anything left depending on a row is not true", "[predicate_fold]") {
	DuckDB db(nullptr);
	Connection con(db);
	CreateUsers(con, "(1, 1), (2, 2), (3, 3)");

	CHECK_FALSE(FoldsToTrue(con, "false"));
	CHECK_FALSE(FoldsToTrue(con, "NULL"));
	CHECK_FALSE(FoldsToTrue(con, "tenant = 1"));
	CHECK_FALSE(FoldsToTrue(con, "'bob' = 'admin' OR tenant = 7"));
	CHECK_FALSE(FoldsToTrue(con, "'admin' = 'admin' AND tenant = 1"));
	CHECK_FALSE(FoldsToTrue(con, "random() < 2"));
	CHECK_FALSE(FoldsToTrue(con, "(SELECT count(*) FROM users) > 0"));
}

TEST_CASE("predicate_fold: the answer does not depend on the rows", "[predicate_fold]") {
	// Statistics would drop `tenant < 100` when every tenant is below 100, and turn `tenant = 7` into an empty
	// result when no row has it.
	for (auto rows : {string(), string("(1, 1), (2, 2), (3, 3)")}) {
		DuckDB db(nullptr);
		Connection con(db);
		CreateUsers(con, rows);

		CHECK_FALSE(FoldsToTrue(con, "tenant < 100"));
		CHECK_FALSE(FoldsToTrue(con, "tenant = 7"));
	}
}
