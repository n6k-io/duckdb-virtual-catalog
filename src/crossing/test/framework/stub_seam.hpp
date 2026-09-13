#pragma once

#include "catch.hpp"
#include "memory_source/memory_source.hpp"
#include "internal/seam.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

constexpr idx_t STUB_SEAM_INDEX = 30;
constexpr idx_t STUB_ABOVE_INDEX = 31;
constexpr idx_t STUB_ROWS_INDEX = 32;

inline unique_ptr<LogicalOperator> SeamNode(vector<LogicalType> types) {
	return MakeSeamNode(STUB_SEAM_INDEX, std::move(types));
}

inline unique_ptr<LogicalOperator> Rows(vector<LogicalType> types = {LogicalType::INTEGER, LogicalType::INTEGER}) {
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t i = 0; i < types.size(); i++) {
		chunk.SetValue(i, 0, Value(types[i]));
	}
	chunk.SetCardinality(1);
	collection->Append(chunk);
	return make_uniq<LogicalColumnDataGet>(STUB_ROWS_INDEX, std::move(types), std::move(collection));
}

inline shared_ptr<CrossingFragment> FragmentOverSeam(vector<LogicalType> seam_types) {
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->seam_types = seam_types;

	auto seam = SeamNode(seam_types);

	vector<unique_ptr<Expression>> select_list;
	auto bindings = seam->GetColumnBindings();
	auto &get = seam->Cast<LogicalGet>();
	for (idx_t i = 0; i < bindings.size(); i++) {
		select_list.push_back(make_uniq<BoundColumnRefExpression>(get.names[i], get.returned_types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(STUB_ABOVE_INDEX, std::move(select_list));
	projection->children.push_back(std::move(seam));
	fragment->plan = std::move(projection);
	fragment->ResolveTypesAndText();
	return fragment;
}

inline CrossingSource &StubSource() {
	return MemorySourceFor("memory", {});
}

} // namespace duckdb
