#include "internal/pass.hpp"
#include "internal/seam_split.hpp"
#include "framework/bound_plan.hpp"
#include "framework/plan_dsl.hpp"
#include "framework/stub_seam.hpp"

using namespace duckdb;

namespace {

struct ShapedWrite {
	shared_ptr<CrossingFragment> fragment;
	SeamSplit split;
};

ShapedWrite ShapeBoundWrite(unique_ptr<LogicalOperator> &plan, vector<LogicalType> seam_types,
                            vector<column_t> key_columns = {}) {
	ShapedWrite shaped;
	shaped.fragment = FragmentOverSeam(std::move(seam_types));
	auto row = SeamRowOf(*plan, key_columns, std::move(plan->children[0]));
	shaped.split = SplitFeedIntoSeam(std::move(row.plan), *shaped.fragment, StubSource(), SeamStop());
	return shaped;
}

} // namespace

TEST_CASE("SELECT amt FROM memory_source() WHERE amt > 100", "[bound]") {
	Bound env;

	auto plan = env.Bind("SELECT amt FROM memory_source() WHERE amt > 100");

	// The cast is not noise. This is the plan before duckdb's optimizers, so the literal has not been
	// folded to its column's type yet -- which is what a rule sees when the pass runs where it runs.
	REQUIRE_PLAN(plan, "proj{amt} | filter{amt > CAST(100 AS INTEGER)} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	// The fragment was built with both columns and narrowed to the one the scan asks for.
	REQUIRE_PLAN(plan, "crossing[proj{amt} | filter{amt > CAST(100 AS INTEGER)} | proj{amt} | scan]");
}

TEST_CASE("SELECT count(*) FROM memory_source()", "[bound]") {
	Bound env {{"count_star"}};

	auto plan = env.Bind("SELECT count(*) FROM memory_source()");

	REQUIRE_PLAN(plan, "proj{count_star()} | agg{count_star()} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[proj{count_star()} | agg{count_star()} | proj{id} | scan]");
}

TEST_CASE("SELECT count(*) FROM memory_source(), by a source that does not know count", "[bound]") {
	Bound env;

	auto plan = env.Bind("SELECT count(*) FROM memory_source()");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "proj{count_star()} | agg{count_star()} | crossing[proj{id} | scan]");
}

TEST_CASE("SELECT id, count(amt) FROM memory_source() GROUP BY id", "[bound]") {
	Bound env {{"count"}};

	auto plan = env.Bind("SELECT id, count(amt) FROM memory_source() GROUP BY id");

	MoveCrossableWorkIntoFragments(plan);

	REQUIRE_PLAN(plan, "crossing[proj{id, count(amt)} | agg{count(amt) by id} | proj{id, amt} | scan]");
}

TEST_CASE("SELECT amt FROM memory_source() WHERE amt > 100 AND amt + 1 > 2", "[bound]") {
	Bound env;

	auto plan = env.Bind("SELECT amt FROM memory_source() WHERE amt > 100 AND amt + 1 > 2");

	MoveCrossableWorkIntoFragments(plan);

	// `+` binds as a function call, and this source answers for no function, so that conjunct stays
	// while the one beside it crosses. The split, on a plan the planner produced.
	REQUIRE_PLAN(plan, "proj{amt} | filter{(amt + 1) > CAST(2 AS INTEGER)} | "
	                   "crossing[filter{amt > CAST(100 AS INTEGER)} | proj{amt} | scan]");
}

TEST_CASE("INSERT INTO t VALUES (1, 2)", "[bound][split]") {
	Bound env;

	auto plan = env.Bind("INSERT INTO t VALUES (1, 2)");
	REQUIRE_PLAN(plan, "insert | proj{id, amt} | rows | local");

	auto shaped = ShapeBoundWrite(plan, {LogicalType::INTEGER, LogicalType::INTEGER});

	REQUIRE(!shaped.split.remainder);
	REQUIRE_FRAGMENT(*shaped.fragment, "proj{c0, c1} | proj{id, amt} | proj{id, amt} | rows | scan");
}

TEST_CASE("INSERT INTO t VALUES (1, 2), (3, 4)", "[bound][split]") {
	Bound env;

	auto plan = env.Bind("INSERT INTO t VALUES (1, 2), (3, 4)");
	REQUIRE_PLAN(plan, "insert | proj{id, amt} | rows | local");

	auto shaped = ShapeBoundWrite(plan, {LogicalType::INTEGER, LogicalType::INTEGER});

	REQUIRE(!shaped.split.remainder);
	REQUIRE_FRAGMENT(*shaped.fragment, "proj{c0, c1} | proj{id, amt} | proj{id, amt} | rows | scan");
}

TEST_CASE("INSERT INTO t SELECT id, amt FROM memory_source()", "[bound][split]") {
	Bound env;

	auto plan = env.Bind("INSERT INTO t SELECT id, amt FROM memory_source()");
	REQUIRE_PLAN(plan, "insert | proj{id, amt} | crossing[proj{id, amt} | scan]");

	MoveCrossableWorkIntoFragments(plan);
	REQUIRE_PLAN(plan, "insert | crossing[proj{id, amt} | proj{id, amt} | scan]");

	auto shaped = ShapeBoundWrite(plan, {LogicalType::INTEGER, LogicalType::INTEGER});

	REQUIRE(!shaped.split.remainder);
	REQUIRE_FRAGMENT(*shaped.fragment, "proj{c0, c1} | proj{c0, c1} | proj{id, amt} | proj{id, amt} | scan");
}

TEST_CASE("UPDATE t SET amt = 1 WHERE id = 2", "[bound][split]") {
	Bound env;

	auto plan = env.Bind("UPDATE t SET amt = 1 WHERE id = 2");
	REQUIRE_PLAN(plan, "update{#[1.0]} | proj{CAST(1 AS INTEGER), rowid} | "
	                   "filter{id = CAST(2 AS INTEGER)} | local");

	auto shaped = ShapeBoundWrite(plan, {LogicalType::BIGINT, LogicalType::INTEGER}, {0});

	REQUIRE_PLAN(shaped.split.remainder, "local");
	REQUIRE_FRAGMENT(*shaped.fragment, "proj{c0, c1} | proj{rowid, CAST(1 AS INTEGER)} | "
	                                   "proj{CAST(1 AS INTEGER), rowid} | filter{id = CAST(2 AS INTEGER)} | seam");
	REQUIRE(shaped.split.obstacle == "get stays on the target");
}

TEST_CASE("DELETE FROM t WHERE id = 2", "[bound][split]") {
	Bound env;

	auto plan = env.Bind("DELETE FROM t WHERE id = 2");
	REQUIRE_PLAN(plan, "delete{rowid} | filter{id = CAST(2 AS INTEGER)} | local");

	auto shaped = ShapeBoundWrite(plan, {LogicalType::BIGINT}, {0});

	REQUIRE_PLAN(shaped.split.remainder, "local");
	REQUIRE_FRAGMENT(*shaped.fragment, "proj{c0} | proj{rowid} | filter{id = CAST(2 AS INTEGER)} | seam");
}

TEST_CASE("SELECT a.amt FROM memory_source() a, memory_source() b WHERE a.id = b.id", "[bound]") {
	Bound env;

	auto plan = env.Bind("SELECT a.amt FROM memory_source() a, memory_source() b WHERE a.id = b.id");

	// A cross product with the join condition still above it: duckdb turns this into a join, but that
	// is one of its optimizers and this is the plan before them.
	REQUIRE_PLAN(plan, "proj{amt} | filter{id = id} | "
	                   "cross(crossing[proj{id, amt} | scan], crossing[proj{id, amt} | scan])");

	MoveCrossableWorkIntoFragments(plan);

	// One region. The right-hand scan is only read for its id, so alignment narrowed it to that.
	REQUIRE_PLAN(plan, "crossing[proj{amt} | filter{id = id} | "
	                   "cross(proj{id, amt} | scan, proj{id} | scan)]");
}
