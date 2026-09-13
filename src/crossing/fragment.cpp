#include "internal/fragment.hpp"

#include "internal/seam.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

CrossingFragment::~CrossingFragment() {
	plan.reset();
	floor.reset();
}

unique_ptr<LogicalOperator> *CrossingFragment::SeamSlot() {
	return FindSeamSlot(plan);
}

void CrossingFragment::SealFloor(const LogicalOperator &node) {
	if (sealed_floor) {
		throw InternalException("crossing: the floor is already sealed");
	}
	sealed_floor = &node;
}

void CrossingFragment::RebuildPlanForColumns(const vector<column_t> &columns) {
	auto selected = columns;
	if (selected.empty()) {
		if (column_names.empty()) {
			throw InternalException("crossing: the table has no column to count");
		}
		selected.push_back(0);
	}

	vector<unique_ptr<Expression>> select_list;
	output_types.clear();
	projected_columns = selected;
	for (auto column : selected) {
		if (column >= column_names.size()) {
			throw InternalException("crossing: column %llu is not a column of the table", column);
		}
		select_list.push_back(
		    make_uniq<BoundColumnRefExpression>(column_names[column], column_types[column], floor_bindings[column]));
		output_types.push_back(column_types[column]);
	}

	if (!crossing_projection) {
		auto projection = make_uniq<LogicalProjection>(table_index, std::move(select_list));
		projection->children.push_back(std::move(floor));
		crossing_projection = projection.get();
		plan = std::move(projection);
	} else {
		crossing_projection->expressions = std::move(select_list);
	}
	ResolveTypesAndText();
	VerifyInvariants();
}

void CrossingFragment::AdoptTableIndex(idx_t index) {
	if (!plan || !crossing_projection) {
		return;
	}
	table_index = index;
	crossing_projection->table_index = index;
	ResolveTypesAndText();
	VerifyInvariants();
}

void CrossingFragment::ResolveTypesAndText() {
	if (!plan) {
		return;
	}
	plan->ResolveOperatorTypes();
	plan_text = plan->ToString();
}

void CrossingFragment::VerifyInvariants() {
	if (auto slot = SeamSlot()) {
		auto &emitted = (*slot)->types;
		if (emitted.size() != seam_types.size()) {
			throw InternalException("crossing: the seam takes %llu columns, seam_types names %llu", emitted.size(),
			                        seam_types.size());
		}
		for (idx_t i = 0; i < seam_types.size(); i++) {
			if (emitted[i] != seam_types[i]) {
				throw InternalException("crossing: seam column %llu is %s, seam_types names %s", i,
				                        emitted[i].ToString(), seam_types[i].ToString());
			}
		}
	}

	if (!plan || !crossing_projection) {
		return;
	}
	if (sealed_floor && !Contains(*plan, *sealed_floor)) {
		throw InternalException("crossing: the sealed floor is gone from the plan");
	}
	if (crossing_projection->table_index != table_index) {
		throw InternalException("crossing: the projection is on table index %llu, the scan on %llu",
		                        crossing_projection->table_index, table_index);
	}
	auto &emitted = plan->types;
	if (emitted.size() != output_types.size()) {
		throw InternalException("crossing: it emits %llu columns, output_types names %llu", emitted.size(),
		                        output_types.size());
	}
	for (idx_t i = 0; i < output_types.size(); i++) {
		if (emitted[i] != output_types[i]) {
			throw InternalException("crossing: column %llu is %s, output_types names %s", i, emitted[i].ToString(),
			                        output_types[i].ToString());
		}
	}
}

bool CrossingFragment::Contains(const LogicalOperator &haystack, const LogicalOperator &needle) {
	if (&haystack == &needle) {
		return true;
	}
	for (auto &child : haystack.children) {
		if (child && Contains(*child, needle)) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
