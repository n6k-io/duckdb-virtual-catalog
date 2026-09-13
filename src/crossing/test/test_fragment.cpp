#include "catch.hpp"
#include "memory_source/memory_source.hpp"

using namespace duckdb;

namespace {

vector<string> EmittedColumns(const CrossingFragment &fragment) {
	vector<string> out;
	for (auto &expr : fragment.crossing_projection->expressions) {
		out.push_back(expr->GetName());
	}
	return out;
}

} // namespace

TEST_CASE("building the first projection moves the floor into it", "[fragment]") {
	auto fragment = MemoryFragment();

	fragment->RebuildPlanForColumns({0, 1});

	REQUIRE(fragment->plan.get() == fragment->crossing_projection.get());
	REQUIRE(EmittedColumns(*fragment) == vector<string> {"id", "amt"});
	REQUIRE(fragment->output_types == fragment->column_types);
	REQUIRE(fragment->floor == nullptr);
}

TEST_CASE("projecting again reuses the same projection node", "[fragment]") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});
	auto *first = fragment->crossing_projection.get();

	fragment->RebuildPlanForColumns({1});

	REQUIRE(fragment->crossing_projection.get() == first);
	REQUIRE(EmittedColumns(*fragment) == vector<string> {"amt"});
	REQUIRE(fragment->output_types == vector<LogicalType> {LogicalType::INTEGER});
}

TEST_CASE("a narrowed fragment can widen again", "[fragment]") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({1});

	fragment->RebuildPlanForColumns({0, 1});

	REQUIRE(EmittedColumns(*fragment) == vector<string> {"id", "amt"});
}

TEST_CASE("a fragment asked for no columns still emits one", "[fragment]") {
	auto fragment = MemoryFragment();

	fragment->RebuildPlanForColumns({});

	REQUIRE(EmittedColumns(*fragment) == vector<string> {"id"});
}

TEST_CASE("asking for a column again replaces the counting column", "[fragment]") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({});

	fragment->RebuildPlanForColumns({1});

	REQUIRE(EmittedColumns(*fragment) == vector<string> {"amt"});
}

TEST_CASE("a column the source does not have is refused", "[fragment]") {
	auto fragment = MemoryFragment();

	REQUIRE_THROWS_AS(fragment->RebuildPlanForColumns({2}), InternalException);
}

TEST_CASE("adopting a table index moves the projection to it", "[fragment]") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});

	fragment->AdoptTableIndex(7);

	REQUIRE(fragment->table_index == 7);
	REQUIRE(fragment->crossing_projection->table_index == 7);
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("swapping the floor for another node fails verification", "[fragment]") {
	auto fragment = MemoryFragment();
	fragment->RebuildPlanForColumns({0, 1});

	fragment->crossing_projection->children[0] = make_uniq<LogicalDummyScan>(MEMORY_FLOOR_INDEX);

	REQUIRE_THROWS_AS(fragment->VerifyInvariants(), InternalException);
}

TEST_CASE("a floor can only be sealed once", "[fragment]") {
	auto fragment = MemoryFragment();
	LogicalDummyScan another(MEMORY_FLOOR_INDEX);

	REQUIRE_THROWS_AS(fragment->SealFloor(another), InternalException);
}
