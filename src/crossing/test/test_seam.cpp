#include "framework/stub_seam.hpp"

#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

namespace {

vector<LogicalType> TwoIntegers() {
	return {LogicalType::INTEGER, LogicalType::INTEGER};
}

} // namespace

TEST_CASE("the seam slot holds the seam node", "[seam]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto slot = fragment->SeamSlot();

	REQUIRE(slot);
	REQUIRE((*slot)->type == LogicalOperatorType::LOGICAL_GET);
	REQUIRE(slot->get() == fragment->plan->children[0].get());
}

TEST_CASE("a seam at the root is still the seam", "[seam]") {
	CrossingFragment fragment;
	fragment.plan = SeamNode(TwoIntegers());

	REQUIRE(fragment.SeamSlot() == &fragment.plan);
}

TEST_CASE("a fragment with no plan has no seam slot", "[seam]") {
	CrossingFragment fragment;

	REQUIRE(!fragment.SeamSlot());
}

TEST_CASE("writing through the slot puts another node in the seam's place", "[seam]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto replacement = make_uniq<LogicalProjection>(STUB_ABOVE_INDEX + 1, vector<unique_ptr<Expression>>());
	auto *expected = replacement.get();

	*fragment->SeamSlot() = std::move(replacement);

	REQUIRE(fragment->plan->children[0].get() == expected);
	REQUIRE(!fragment->HasSeam());
}

TEST_CASE("a seam of a different width than seam_types fails verification", "[seam]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	fragment->seam_types = {LogicalType::INTEGER};

	REQUIRE_THROWS_AS(fragment->VerifyInvariants(), InternalException);
}

TEST_CASE("a seam of a different type than seam_types fails verification", "[seam]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	fragment->seam_types = {LogicalType::INTEGER, LogicalType::VARCHAR};

	REQUIRE_THROWS_AS(fragment->VerifyInvariants(), InternalException);
}

TEST_CASE("a seam that matches what it was declared to take verifies", "[seam]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}
