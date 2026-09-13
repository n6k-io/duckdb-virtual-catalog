#include "internal/fold.hpp"
#include "framework/plan_builder.hpp"
#include "internal/labelling.hpp"

using namespace duckdb;

namespace {

bool IsLabelled(const SubtreeLabels &labels, LogicalOperator &op) {
	return labels.find(&op) != labels.end();
}

string LabelOf(const SubtreeLabels &labels, LogicalOperator &op) {
	auto found = labels.find(&op);
	if (found == labels.end()) {
		return string();
	}
	for (auto &name : {"memory", "elsewhere"}) {
		if (found->second.source == &MemorySourceFor(name, {})) {
			return name;
		}
	}
	return found->second.runs_anywhere ? "*" : "?";
}

} // namespace

TEST_CASE("a crossing scan names its own source", "[labelling]") {
	auto plan = PlanFromDSL("crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("a leaf that is not a crossing scan names nothing", "[labelling]") {
	auto plan = PlanFromDSL("local");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(labels.empty());
}

TEST_CASE("a filter the source can compute takes the source's name", "[labelling]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(LabelOf(labels, *plan) == "memory");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("a filter the source cannot compute is unnamed, and the scan below it is not", "[labelling]") {
	auto plan = PlanFromDSL("filter{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("an order the source can compute takes the source's name", "[labelling]") {
	auto plan = PlanFromDSL("order{amt} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("an order whose keys the source cannot compute is unnamed, and the scan below it is not", "[labelling]") {
	auto plan = PlanFromDSL("order{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("a join of two scans of one source takes that source's name", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], crossing[proj{id, amt} | scan])");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("a join of two different sources is unnamed, and both scans keep theirs", "[labelling]") {
	auto plan = Build(Join(ScanOf("memory"), ScanOf("elsewhere")));

	auto labels = LabelSubtrees(*plan);

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
	REQUIRE(LabelOf(labels, *plan->children[1]) == "elsewhere");
}

TEST_CASE("a join with one branch on the target is unnamed", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], local)");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("an unnamed node stops everything above it being named", "[labelling]") {
	auto plan = PlanFromDSL("filter{amt > 100} | filter{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(!IsLabelled(labels, *plan->children[0]));
	REQUIRE(LabelOf(labels, *plan->children[0]->children[0]) == "memory");
}

TEST_CASE("what the labelling names is what the fold takes", "[labelling]") {
	auto plan = PlanFromDSL("limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan);
	REQUIRE(LabelOf(labels, *plan) == "memory");

	plan = FoldSubtreeIntoItsFragment(std::move(plan));

	REQUIRE_PLAN(plan, "crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("rows the target holds run anywhere beneath a write", "[labelling]") {
	auto plan = PlanFromDSL("proj{c0, c1} | rows");

	auto labels = LabelSubtreesUnder(*plan, StubSource());

	REQUIRE(LabelOf(labels, *plan) == "*");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "*");
}

TEST_CASE("rows the target holds run anywhere outside a write too", "[labelling]") {
	auto plan = PlanFromDSL("proj{c0, c1} | rows");

	auto labels = LabelSubtrees(*plan);

	REQUIRE(LabelOf(labels, *plan) == "*");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "*");
}

TEST_CASE("rows beside a scan take the scan's source", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], rows)");

	auto under_write = LabelSubtreesUnder(*plan, StubSource());
	REQUIRE(LabelOf(under_write, *plan) == "memory");

	auto under_read = LabelSubtrees(*plan);
	REQUIRE(LabelOf(under_read, *plan) == "memory");
}

TEST_CASE("what stands over the rows is asked of the source they meet", "[labelling]") {
	auto crosses = PlanFromDSL("join(crossing[proj{id, amt} | scan], filter{c0 > 1} | rows)");
	auto stays = PlanFromDSL("join(crossing[proj{id, amt} | scan], filter{target_only(c0)} | rows)");

	REQUIRE(LabelOf(LabelSubtrees(*crosses), *crosses) == "memory");
	REQUIRE(!IsLabelled(LabelSubtrees(*stays), *stays));
}
