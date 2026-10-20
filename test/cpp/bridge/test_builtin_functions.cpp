#include "catch.hpp"
#include "builtin_functions.hpp"

#include "duckdb.hpp"

using namespace duckdb;

TEST_CASE("builtin_functions: every internal scalar and aggregate is classified once", "[builtin_functions]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto result = con.Query("SELECT DISTINCT function_name FROM duckdb_functions() WHERE internal AND function_type "
	                        "IN ('scalar', 'aggregate') ORDER BY 1");
	REQUIRE_FALSE(result->HasError());
	for (idx_t row = 0; row < result->RowCount(); row++) {
		auto name = result->GetValue(0, row).ToString();
		INFO(name);
		CHECK(PushableBuiltins().count(name) + TargetOnlyBuiltins().count(name) == 1);
	}
}
