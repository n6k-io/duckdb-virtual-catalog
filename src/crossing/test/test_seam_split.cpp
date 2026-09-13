#include "internal/seam_split.hpp"
#include "framework/plan_builder.hpp"

using namespace duckdb;

namespace {

vector<LogicalType> TwoIntegers() {
	return {LogicalType::INTEGER, LogicalType::INTEGER};
}

SeamSplit Split(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, const SeamStop &stop = SeamStop()) {
	return SplitFeedIntoSeam(std::move(feed), fragment, StubSource(), stop);
}

} // namespace

TEST_CASE("rows the target holds fill the seam", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE(!split.remainder);
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | rows");
}

TEST_CASE("a whole chain crosses at once", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{true} | proj{c0, c1} | rows"), *fragment);

	REQUIRE(!split.remainder);
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | filter{true} | proj{c0, c1} | rows");
}

TEST_CASE("what fills the seam answers to the seam's index", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE(!fragment->SeamSlot());
	REQUIRE(fragment->plan->children[0]->GetColumnBindings()[0].table_index == STUB_SEAM_INDEX);
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("the fragment still emits what it did before the fill", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto before = fragment->plan->types;

	Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE(fragment->plan->types == before);
}

TEST_CASE("a read of the same source fills the seam with its own plan", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{id, amt} | filter{amt > 100} | crossing[proj{id, amt} | scan]"), *fragment);

	REQUIRE(!split.remainder);
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{id, amt} | filter{amt > 100} | proj{id, amt} | scan");
}

TEST_CASE("a node the source cannot compute is where the feed splits", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "filter{target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | seam");
	REQUIRE(split.obstacle == "filter is not something the source can compute");
	REQUIRE(split.boundary_types == TwoIntegers());
}

TEST_CASE("everything above the split crosses", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split =
	    Split(PlanFromDSL("proj{c0, c1} | filter{true} | proj{c0, c1} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "filter{target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | filter{true} | proj{c0, c1} | seam");
}

TEST_CASE("a mixed filter leaves only the half the source cannot compute", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{c0 > 1, target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "filter{target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | filter{c0 > 1} | seam");
}

TEST_CASE("a read of another source is the boundary", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto feed = Build(Proj("id", "amt"), Filter(Gt(Col("amt"), Int(100))), ScanOf("elsewhere"));

	auto split = Split(std::move(feed), *fragment);

	REQUIRE_PLAN(split.remainder, "crossing[proj{id, amt} | scan]");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{id, amt} | filter{amt > 100} | seam");
	REQUIRE(split.obstacle == "a read of another source stays on the target");
}

TEST_CASE("the seam takes the boundary's shape, not the row's", "[split]") {
	auto fragment = FragmentOverSeam({LogicalType::INTEGER});

	auto split = Split(PlanFromDSL("proj{c0} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE(split.boundary_types == TwoIntegers());
	REQUIRE(fragment->seam_types == TwoIntegers());
	REQUIRE_FRAGMENT(*fragment, "proj{c0} | proj{c0} | seam");
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("the crossed prefix reads the new seam", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	Split(PlanFromDSL("proj{c0, c1} | proj{c0, c1} | filter{target_only(c0)} | rows"), *fragment);

	auto &seam = **fragment->SeamSlot();
	auto seam_index = seam.Cast<LogicalGet>().table_index;
	auto &above = *fragment->plan->children[0]->children[0];
	for (auto &expr : above.expressions) {
		REQUIRE(expr->Cast<BoundColumnRefExpression>().binding.table_index == seam_index);
	}
}

TEST_CASE("a stop at the root keeps the whole feed on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto feed = PlanFromDSL("proj{c0, c1} | rows");
	SeamStop stop;
	stop.node = feed.get();
	stop.reason = "the rows are wanted back";

	auto split = Split(std::move(feed), *fragment, stop);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
	REQUIRE(split.obstacle == "the rows are wanted back");
	REQUIRE(split.boundary_types == TwoIntegers());
}

TEST_CASE("a stop below the root crosses what is above it", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto feed = PlanFromDSL("proj{c0, c1} | proj{c0, c1} | rows");
	SeamStop stop;
	stop.node = feed->children[0].get();
	stop.reason = "the rows are wanted back";

	auto split = Split(std::move(feed), *fragment, stop);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{c0, c1} | seam");
}

TEST_CASE("a stop at the root of a row the seam does not take is refused", "[split]") {
	auto fragment = FragmentOverSeam({LogicalType::INTEGER});
	auto feed = PlanFromDSL("proj{c0, c1} | rows");
	SeamStop stop;
	stop.node = feed.get();

	REQUIRE_THROWS_AS(Split(std::move(feed), *fragment, stop), InternalException);
}

TEST_CASE("a fragment with no seam cannot be split into", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE_THROWS_AS(Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment), InternalException);
}
