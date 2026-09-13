#include "internal/pass.hpp"
#include "framework/plan_builder.hpp"

using namespace duckdb;

TEST_CASE("narrowing stacks a projection rather than rewriting the one below", "[narrow]") {
	auto scan = Scan();
	Align(scan);
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});

	Narrow(scan);

	REQUIRE_PLAN(scan, "crossing[proj{amt} | proj{id, amt} | scan]");
}

TEST_CASE("a fragment that already emits what the scan wants is left alone", "[narrow]") {
	auto scan = Scan();
	Align(scan);

	Narrow(scan);

	REQUIRE_PLAN(scan, "crossing[proj{id, amt} | scan]");
}

TEST_CASE("narrowing a folded fragment leaves what crossed untouched", "[narrow]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");
	MoveCrossableWorkIntoFragments(plan);
	REQUIRE_PLAN(plan, "crossing[filter{amt > 100} | proj{id, amt} | scan]");

	plan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});
	NarrowFragmentsToTheirScans(plan);

	REQUIRE_PLAN(plan, "crossing[proj{c1} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("the scan left behind emits what the narrowed fragment does", "[narrow]") {
	auto scan = Scan();
	Align(scan);
	scan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});

	Narrow(scan);

	auto fragment = CrossingReadFragmentOf(*scan);
	REQUIRE(fragment->output_types == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(scan->Cast<LogicalGet>().returned_types == fragment->output_types);
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("narrowing to nothing leaves the fragment as it was", "[narrow]") {
	auto scan = Scan();
	Align(scan);
	scan->Cast<LogicalGet>().SetColumnIds({});

	Narrow(scan);

	REQUIRE_PLAN(scan, "crossing[proj{id, amt} | scan]");
}

TEST_CASE("narrowing twice stacks only what the second one needs", "[narrow]") {
	auto plan = Build(Scan());
	Align(plan);
	plan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(0), ColumnIndex(1)});
	NarrowFragmentsToTheirScans(plan);

	plan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});
	NarrowFragmentsToTheirScans(plan);

	REQUIRE_PLAN(plan, "crossing[proj{amt} | proj{id, amt} | scan]");
}
