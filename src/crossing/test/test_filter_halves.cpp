#include "internal/fold.hpp"
#include "internal/filter_halves.hpp"
#include "framework/plan_builder.hpp"
#include "internal/labelling.hpp"

using namespace duckdb;

TEST_CASE("a mixed filter splits, with the computable half nearest the crossing", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100, target_only(amt)} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);

	REQUIRE_PLAN(plan, "filter{target_only(amt)} | filter{amt > 100} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("a filter the source can compute whole does not split", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100, id > 1} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);

	REQUIRE_PLAN(plan, "filter{amt > 100, id > 1} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("a filter the source can compute none of does not split", "[split]") {
	auto plan = PlanFromDSL("filter{a(amt), b(id)} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);

	REQUIRE_PLAN(plan, "filter{a(amt), b(id)} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("a filter with no crossing below it is left whole", "[split]") {
	auto plan = PlanFromDSL("filter{true, false} | local");

	SplitFiltersAtEvaluableHalf(plan);

	REQUIRE_PLAN(plan, "filter{true, false} | local");
}

TEST_CASE("a function the source knows counts as computable", "[split]") {
	auto plan = Build(Filter(UnknownFn("shift", Col("amt")), UnknownFn("target_only", Col("amt"))), Scan({"shift"}));
	REQUIRE_PLAN(plan, "filter{shift(amt), target_only(amt)} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);

	REQUIRE_PLAN(plan, "filter{target_only(amt)} | filter{shift(amt)} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("splitting lets the computable half cross while the rest stays", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100, target_only(amt)} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);
	auto labels = LabelSubtrees(*plan);
	REQUIRE(labels.find(plan.get()) == labels.end());
	REQUIRE(labels.find(plan->children[0].get()) != labels.end());

	plan->children[0] = FoldSubtreeIntoItsFragment(std::move(plan->children[0]));

	REQUIRE_PLAN(plan, "filter{target_only(amt)} | crossing[filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("rejoining puts a split filter back into one node, in the split's order", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100, target_only(amt)} | crossing[proj{id, amt} | scan]");

	SplitFiltersAtEvaluableHalf(plan);
	RejoinAdjacentFiltersEverywhere(plan);

	REQUIRE_PLAN(plan, "filter{target_only(amt), amt > 100} | crossing[proj{id, amt} | scan]");
}

TEST_CASE("rejoining merges filters inside a fragment too", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100} | filter{id > 1} | crossing[proj{id, amt} | scan]");
	plan = FoldSubtreeIntoItsFragment(std::move(plan));
	REQUIRE_PLAN(plan, "crossing[filter{amt > 100} | filter{id > 1} | proj{id, amt} | scan]");

	RejoinAdjacentFiltersEverywhere(plan);

	REQUIRE_PLAN(plan, "crossing[filter{amt > 100, id > 1} | proj{id, amt} | scan]");
}

TEST_CASE("rejoining leaves a filter with something between it and the next alone", "[split]") {
	auto plan = PlanFromDSL("filter{amt > 100} | proj{amt} | filter{id > 1} | crossing[proj{id, amt} | scan]");

	RejoinAdjacentFiltersEverywhere(plan);

	REQUIRE_PLAN(plan, "filter{amt > 100} | proj{amt} | filter{id > 1} | crossing[proj{id, amt} | scan]");
}
