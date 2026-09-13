#include "internal/fold.hpp"
#include "framework/plan_builder.hpp"

using namespace duckdb;

TEST_CASE("a filter folds into the fragment below it", "[fold]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("a stack of operators folds in one step", "[fold]") {
	auto plan = PlanFromDSL("limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("a projection folds in and keeps its expressions", "[fold]") {
	auto plan = PlanFromDSL("proj{amt > 100} | crossing[proj{id, amt} | scan]");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[proj{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("a subtree that is already a crossing scan is left alone", "[fold]") {
	auto plan = PlanFromDSL("crossing[proj{id, amt} | scan]");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[proj{id, amt} | scan]");
}

TEST_CASE("a subtree with no crossing scan in it is refused", "[fold]") {
	auto plan = PlanFromDSL("filter{true} | local");

	REQUIRE_THROWS_AS(FoldSubtreeIntoItsFragment(std::move(plan)), InternalException);
}

TEST_CASE("the sealed floor survives the fold", "[fold]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");
	auto fragment = CrossingReadFragmentOf(*plan->children[0]);

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[filter{amt > 100} | proj{id, amt} | scan]");
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("the scan left behind emits what the subtree emitted", "[fold]") {
	auto plan = PlanFromDSL("proj{amt} | crossing[proj{id, amt} | scan]");
	auto before = plan->types;

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[proj{amt} | proj{id, amt} | scan]");
	REQUIRE(plan->types == before);
	REQUIRE(plan->Cast<LogicalGet>().returned_types == before);
}

TEST_CASE("the scan left behind answers to the subtree root's index", "[fold]") {
	auto plan = PlanFromDSL("proj{amt} | crossing[proj{id, amt} | scan]");
	auto root_index = plan->GetColumnBindings()[0].table_index;

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[proj{amt} | proj{id, amt} | scan]");
	REQUIRE(plan->Cast<LogicalGet>().table_index == root_index);
	REQUIRE(plan->GetColumnBindings()[0].table_index == root_index);
}

TEST_CASE("the fragment reports what it now emits", "[fold]") {
	auto plan = PlanFromDSL("proj{amt} | crossing[proj{id, amt} | scan]");
	auto fragment = CrossingReadFragmentOf(*plan->children[0]);

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[proj{amt} | proj{id, amt} | scan]");
	REQUIRE(fragment->output_types == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(fragment->plan->types == fragment->output_types);
}

TEST_CASE("folding twice stacks the second subtree above the first", "[fold]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");
	plan = FoldSubtreeIntoItsFragment(std::move(plan));
	REQUIRE_PLAN(plan, "crossing[filter{amt > 100} | proj{id, amt} | scan]");

	plan = Build(Limit(5), std::move(plan));
	REQUIRE_PLAN(plan, "limit{5} | crossing[filter{amt > 100} | proj{id, amt} | scan]");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("a folded fragment holds its invariants", "[fold]") {
	auto plan = PlanFromDSL("proj{amt} | crossing[proj{id, amt} | scan]");
	auto fragment = CrossingReadFragmentOf(*plan->children[0]);
	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("a folded fragment still notices when what it emits changes underneath it", "[fold]") {
	auto plan = PlanFromDSL("proj{amt} | crossing[proj{id, amt} | scan]");
	auto fragment = CrossingReadFragmentOf(*plan->children[0]);
	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	fragment->output_types = {LogicalType::INTEGER, LogicalType::INTEGER};

	REQUIRE_THROWS_AS(fragment->VerifyInvariants(), InternalException);
}
