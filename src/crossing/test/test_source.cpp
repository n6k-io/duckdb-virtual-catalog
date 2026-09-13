#include "catch.hpp"
#include "memory_source/memory_source.hpp"

using namespace duckdb;

TEST_CASE("a crossing scan hands back its fragment", "[source]") {
	auto scan = MemorySourceScan();

	auto fragment = CrossingReadFragmentOf(*scan);

	REQUIRE(fragment);
	REQUIRE(fragment->plan);
	REQUIRE(fragment->column_names == vector<string> {"id", "amt"});
}

TEST_CASE("an operator that is not a crossing scan hands back nothing", "[source]") {
	auto scan = make_uniq<LogicalDummyScan>(MEMORY_FLOOR_INDEX);

	REQUIRE(!CrossingReadFragmentOf(*scan));
	REQUIRE(!CrossingSourceOf(*scan));
}

TEST_CASE("a GET with no bind data is not a crossing scan", "[source]") {
	TableFunction function("plain_scan", {}, nullptr);
	auto get = make_uniq<LogicalGet>(MEMORY_GET_INDEX, function, nullptr, vector<LogicalType> {LogicalType::INTEGER},
	                                 vector<string> {"id"});

	REQUIRE(!CrossingReadFragmentOf(*get));
}

TEST_CASE("a source answers only for the functions it was given", "[source]") {
	auto scan = MemorySourceScan({"shift"});

	auto source = CrossingSourceOf(*scan);

	REQUIRE(source);
	REQUIRE(source->AcceptsCall(*Call("shift")).ok);
	REQUIRE(!source->AcceptsCall(*Call("target_only")).ok);
}

TEST_CASE("a source given nothing refuses every call", "[source]") {
	MemorySource deny;

	REQUIRE(!deny.AcceptsCall(*Call("shift")).ok);
	REQUIRE(!deny.AcceptsCall(*Call("upper")).ok);
}

TEST_CASE("two scans of one source share it", "[source]") {
	auto first = MemorySourceScan();
	auto second = MemorySourceScan();

	REQUIRE(CrossingSourceOf(*first).get() == CrossingSourceOf(*second).get());
}

TEST_CASE("two scans of different sources do not share one", "[source]") {
	auto first = MemorySourceScan();
	auto second = MemorySourceScan({}, "elsewhere");

	REQUIRE(CrossingSourceOf(*first).get() != CrossingSourceOf(*second).get());
}
