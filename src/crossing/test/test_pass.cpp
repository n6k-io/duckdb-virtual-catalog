#include "internal/pass.hpp"
#include "framework/plan_builder.hpp"

using namespace duckdb;

TEST_CASE("a plan with no crossing in it is untouched", "[pass]") {
	auto plan = PlanFromDSL("filter{true} | local");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "filter{true} | local");
}

TEST_CASE("a whole crossable plan becomes one crossing scan", "[pass]") {
	auto plan = PlanFromDSL("limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("the pass folds the largest crossable subtree, not the smallest", "[pass]") {
	auto plan = PlanFromDSL("order{target_only(amt)} | limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "order{target_only(amt)} | crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("an order the source can compute folds in with everything under it", "[pass]") {
	auto plan = PlanFromDSL("order{amt} | limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[order{amt} | limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("a window the source knows folds in", "[pass]") {
	auto plan = Build(RowNumberOver(Col("amt")), Scan({"row_number"}));

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[window{ROW_NUMBER() OVER (ORDER BY amt ASC NULLS LAST)} | proj{id, amt} | scan]");
}

TEST_CASE("a window the source does not know stays", "[pass]") {
	auto plan = Build(RowNumberOver(Col("amt")), Scan());

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "window{ROW_NUMBER() OVER (ORDER BY amt ASC NULLS LAST)} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("an unnest folds in with what it unnests", "[pass]") {
	auto plan = Build(Unnest(Col("amt")), Scan());

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[unnest{UNNEST(amt)} | proj{id, amt} | scan]");
}

TEST_CASE("a filter holding a case folds in", "[pass]") {
	auto plan = Build(Filter(Gt(CaseWhen(Gt(Col("amt"), Int(0)), Int(1), Int(0)), Int(0))), Scan());

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[filter{CASE  WHEN ((amt > 0)) THEN (1) ELSE 0 END > 0} | proj{id, amt} | scan]");
}

TEST_CASE("an operator the source cannot compute stays above the crossing", "[pass]") {
	auto plan = PlanFromDSL("filter{target_only(amt)} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "filter{target_only(amt)} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("a mixed filter is split, and only the computable half crosses", "[pass]") {
	auto plan = PlanFromDSL("filter{amt > 100, target_only(amt)} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "filter{target_only(amt)} | crossing[filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("filters that both cross come back as one node", "[pass]") {
	auto plan = PlanFromDSL("filter{amt > 100} | filter{id > 1} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[filter{amt > 100, id > 1} | proj{id, amt} | scan]");
}

TEST_CASE("a join with a branch on the target keeps the join outside", "[pass]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], local)");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "join(crossing[proj{id, amt} | scan], local)");
}

TEST_CASE("rows the target holds fold in beside a scan", "[pass]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], rows)");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[join(proj{id, amt} | scan, rows)]");
}

TEST_CASE("rows with no scan beside them have nowhere to go", "[pass]") {
	auto plan = PlanFromDSL("join(proj{c0, c1} | rows, local)");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "join(proj{c0, c1} | rows, local)");
}

TEST_CASE("a join of two different sources keeps the join outside", "[pass]") {
	auto plan = Build(Join(ScanOf("memory"), ScanOf("elsewhere")));

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "join(crossing[proj{id, amt} | scan], crossing[proj{id, amt} | scan])");
}

TEST_CASE("a join of two scans of one source folds into a single fragment", "[pass]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], crossing[proj{id, amt} | scan])");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[join(proj{id, amt} | scan, proj{id, amt} | scan)]");
}

TEST_CASE("two separate crossings each fold on their own", "[pass]") {
	auto plan = Build(Join(Build(Filter(Gt(Col("amt"), Int(100))), ScanOf("memory")),
	                       Build(Filter(Gt(Col("amt"), Int(200))), ScanOf("elsewhere"))));

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "join(crossing[filter{amt > 100} | proj{id, amt} | scan], "
	                   "crossing[filter{amt > 200} | proj{id, amt} | scan])");
}

TEST_CASE("a projection that widens crosses anyway", "[pass]") {
	auto plan = PlanFromDSL("proj{id, amt, 1} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[proj{id, amt, 1} | proj{id, amt} | scan]");
}

TEST_CASE("the fragment follows the columns the scan is asked for", "[pass]") {
	auto plan = Build(Scan());
	plan->Cast<LogicalGet>().SetColumnIds({ColumnIndex(1)});

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[proj{amt} | scan]");
}
