#include "internal/seam.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types) {
	vector<string> names;
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		names.push_back("c" + to_string(i));
		ids.push_back(ColumnIndex(i));
	}
	TableFunction function(CROSSING_SEAM_FUNCTION, {}, nullptr);
	auto get = make_uniq<LogicalGet>(table_index, function, nullptr, std::move(types), std::move(names));
	get->SetColumnIds(std::move(ids));
	return std::move(get);
}

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return nullptr;
	}
	if (plan->type == LogicalOperatorType::LOGICAL_GET &&
	    plan->Cast<LogicalGet>().function.name == CROSSING_SEAM_FUNCTION) {
		return &plan;
	}
	for (auto &child : plan->children) {
		if (auto found = FindSeamSlot(child)) {
			return found;
		}
	}
	return nullptr;
}

} // namespace duckdb
