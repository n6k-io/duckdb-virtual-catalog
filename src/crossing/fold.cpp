#include "internal/fold.hpp"

#include "internal/fragment.hpp"
#include "internal/source.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

//! Slots rather than scans, so each can be swapped for the plan it stands for without knowing what
//! holds it. More than one means the subtree spans several scans of one source; all fold into the
//! first.
void CollectScanSlots(unique_ptr<LogicalOperator> &node, vector<unique_ptr<LogicalOperator> *> &out) {
	if (CrossingReadFragmentOf(*node)) {
		out.push_back(&node);
		return;
	}
	for (auto &child : node->children) {
		CollectScanSlots(child, out);
	}
}

//! The node whose index the feed's bindings carry: the root, or what a chain of index-less nodes
//! such as filters and limits passes up from below.
LogicalOperator &FirstIndexedNodeBelow(LogicalOperator &root) {
	optional_ptr<LogicalOperator> node = &root;
	while (node->GetTableIndex().empty() && node->children.size() == 1) {
		node = node->children[0].get();
	}
	return *node;
}

} // namespace

//! Whatever fills the seam has to answer to the seam's index, since the plan above still reads the
//! seam's bindings.
void AdoptSeamIndex(LogicalOperator &feed, idx_t seam_index) {
	auto &owner = FirstIndexedNodeBelow(feed);
	switch (owner.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		owner.Cast<LogicalProjection>().table_index = seam_index;
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		owner.Cast<LogicalColumnDataGet>().table_index = seam_index;
		break;
	default:
		throw InternalException("crossing: a %s cannot take the seam's index", LogicalOperatorToString(owner.type));
	}
	for (auto &binding : feed.GetColumnBindings()) {
		if (binding.table_index != seam_index) {
			throw InternalException("crossing: the feed still answers to index %llu after adopting the seam's %llu",
			                        binding.table_index, seam_index);
		}
	}
}

bool SubtreeHoldsFrozenScan(LogicalOperator &node) {
	auto fragment = CrossingReadFragmentOf(node);
	if (fragment && fragment->frozen) {
		return true;
	}
	for (auto &child : node.children) {
		if (SubtreeHoldsFrozenScan(*child)) {
			return true;
		}
	}
	return false;
}

void SpliceReadRegionsIntoPlace(unique_ptr<LogicalOperator> &subtree) {
	vector<unique_ptr<LogicalOperator> *> slots;
	CollectScanSlots(subtree, slots);
	for (auto slot : slots) {
		auto scan = std::move(*slot);
		*slot = std::move(CrossingReadFragmentOf(*scan)->plan);
	}
}

unique_ptr<LogicalOperator> FoldSubtreeIntoItsFragment(unique_ptr<LogicalOperator> subtree) {
	if (CrossingReadFragmentOf(*subtree)) {
		return subtree;
	}
	vector<unique_ptr<LogicalOperator> *> slots;
	CollectScanSlots(subtree, slots);
	if (slots.empty()) {
		throw InternalException("crossing: the subtree holds no crossing scan to fold into");
	}

	// Read what the subtree presents before taking it apart: the plan above names the subtree's root,
	// so the scan standing in its place has to answer to the root's index and shape.
	subtree->ResolveOperatorTypes();
	auto types = subtree->types;
	auto bindings = subtree->GetColumnBindings();

	// Bindings inside the subtree already name each scan's index, and every fragment's projection was
	// given that same index by AdoptTableIndex -- so the splices need no rewriting.
	unique_ptr<LogicalOperator> host;
	optional_ptr<CrossingFragment> host_fragment;
	for (auto slot : slots) {
		auto scan = std::move(*slot);
		auto fragment = CrossingReadFragmentOf(*scan);
		*slot = std::move(fragment->plan);
		if (!host) {
			host = std::move(scan);
			host_fragment = fragment;
		}
	}

	auto &get = host->Cast<LogicalGet>();
	auto root_index = bindings.empty() ? get.table_index : bindings[0].table_index;

	host_fragment->plan = std::move(subtree);
	host_fragment->ResolveTypesAndText();
	host_fragment->output_types = host_fragment->plan->types;
	// The scan above is rebuilt from what the folded subtree emits, so its column ids index that
	// output directly rather than naming table columns.
	host_fragment->projected_columns.clear();
	for (idx_t i = 0; i < host_fragment->output_types.size(); i++) {
		host_fragment->projected_columns.push_back(i);
	}

	get.table_index = root_index;
	get.returned_types = types;
	get.names.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.names.push_back("c" + to_string(i));
	}
	get.projection_ids.clear();
	vector<ColumnIndex> column_ids;
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		column_ids.emplace_back(i);
	}
	get.SetColumnIds(std::move(column_ids));
	get.ResolveOperatorTypes();
	host_fragment->VerifyInvariants();
	return host;
}

} // namespace duckdb
