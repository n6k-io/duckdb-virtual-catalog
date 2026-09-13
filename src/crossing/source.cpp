#include "internal/source.hpp"

#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

optional_ptr<CrossingReadCarrier> ReadCarrierOf(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!get.bind_data) {
		return nullptr;
	}
	if (!get.table_filters.filters.empty() || get.dynamic_filters || !get.projected_input.empty() ||
	    get.ordinality_idx.IsValid()) {
		return nullptr;
	}
	return dynamic_cast<CrossingReadCarrier *>(get.bind_data.get());
}

optional_ptr<CrossingWriteCarrier> WriteCarrierOf(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!get.bind_data) {
		return nullptr;
	}
	return dynamic_cast<CrossingWriteCarrier *>(get.bind_data.get());
}

} // namespace

optional_ptr<CrossingFragment> CrossingReadFragmentOf(LogicalOperator &op) {
	auto carrier = ReadCarrierOf(op);
	return carrier ? carrier->GetReadFragment() : nullptr;
}

optional_ptr<CrossingFragment> CrossingWriteFragmentOf(LogicalOperator &op) {
	auto carrier = WriteCarrierOf(op);
	return carrier ? carrier->GetWriteFragment() : nullptr;
}

optional_ptr<CrossingSource> CrossingSourceOf(LogicalOperator &op) {
	if (auto read = ReadCarrierOf(op)) {
		return &read->Source();
	}
	if (auto write = WriteCarrierOf(op)) {
		return &write->Source();
	}
	return nullptr;
}

} // namespace duckdb
