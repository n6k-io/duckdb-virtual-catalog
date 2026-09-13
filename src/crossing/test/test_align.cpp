#include "framework/plan_builder.hpp"

using namespace duckdb;

TEST_CASE("the fragment emits the columns the scan names", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});

	Align(scan);

	REQUIRE_PLAN(scan, "crossing[proj{amt} | scan]");
}

TEST_CASE("the fragment emits them in the order the scan names them", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1), ColumnIndex(0)});

	Align(scan);

	REQUIRE_PLAN(scan, "crossing[proj{amt, id} | scan]");
}

TEST_CASE("a column past the end of the source table is dropped", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1), ColumnIndex(7)});

	Align(scan);

	REQUIRE_PLAN(scan, "crossing[proj{amt} | scan]");
}

TEST_CASE("a scan naming nothing the source has falls back to counting", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(7)});

	Align(scan);

	REQUIRE_PLAN(scan, "crossing[proj{id} | scan]");
}

TEST_CASE("the fragment takes the scan's table index", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().table_index = 41;

	Align(scan);

	auto fragment = CrossingReadFragmentOf(*scan);
	REQUIRE(fragment->table_index == 41);
	REQUIRE(fragment->crossing_projection->table_index == 41);
}

TEST_CASE("aligning again follows the scan", "[align]") {
	auto scan = Scan();
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});
	Align(scan);

	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(0), ColumnIndex(1)});
	Align(scan);

	REQUIRE_PLAN(scan, "crossing[proj{id, amt} | scan]");
}
