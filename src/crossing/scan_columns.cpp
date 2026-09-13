#include "internal/scan_columns.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

void AlignFragmentToScan(LogicalGet &get, CrossingFragment &fragment) {
	if (!fragment.crossing_projection && !fragment.floor) {
		return;
	}

	vector<column_t> columns;
	for (auto &column_index : get.GetColumnIds()) {
		if (column_index.IsVirtualColumn()) {
			continue;
		}
		auto column = column_index.GetPrimaryIndex();
		if (column >= fragment.column_names.size()) {
			continue;
		}
		columns.push_back(column);
	}

	fragment.RebuildPlanForColumns(columns);
	fragment.AdoptTableIndex(get.table_index);
}

void NarrowFragmentAndScanToRequestedColumns(LogicalGet &get, CrossingFragment &fragment) {
	if (!fragment.plan) {
		return;
	}
	fragment.plan->ResolveOperatorTypes();
	auto bindings = fragment.plan->GetColumnBindings();
	auto &emitted = fragment.plan->types;

	vector<idx_t> wanted;
	for (auto &column_index : get.GetColumnIds()) {
		if (column_index.IsVirtualColumn()) {
			continue;
		}
		auto column = column_index.GetPrimaryIndex();
		if (column < bindings.size()) {
			wanted.push_back(column);
		}
	}
	// Bail unless every id mapped: dropping one renumbers the columns the scan emits while the plan
	// above goes on naming the old positions. A window function over a dropped column catches it.
	if (wanted.size() != get.GetColumnIds().size()) {
		return;
	}
	if (wanted.empty() || wanted.size() == bindings.size()) {
		return;
	}

	vector<unique_ptr<Expression>> select_list;
	vector<LogicalType> types;
	for (auto column : wanted) {
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(get.names[column], emitted[column], bindings[column]));
		types.push_back(emitted[column]);
	}

	auto projection = make_uniq<LogicalProjection>(get.table_index, std::move(select_list));
	projection->children.push_back(std::move(fragment.plan));
	fragment.plan = std::move(projection);
	fragment.ResolveTypesAndText();
	fragment.output_types = std::move(types);

	get.returned_types = fragment.output_types;
	get.names.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.names.push_back("c" + to_string(i));
	}
	get.projection_ids.clear();
	vector<ColumnIndex> column_ids;
	fragment.projected_columns.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		column_ids.emplace_back(i);
		fragment.projected_columns.push_back(i);
	}
	get.SetColumnIds(std::move(column_ids));
	get.ResolveOperatorTypes();
	fragment.VerifyInvariants();
}

} // namespace duckdb
