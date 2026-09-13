#pragma once

#include "provider_scan_shared.hpp"
#include "vcat_pk_buffer.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

// A row id means nothing except relative to the buffer it was allocated from, so the buffer -- not
// the scan -- is what has to match the DML target: one per target table, shared by every scan of it
// in the plan. Scans of other tables keep their own private row-id counter.
inline shared_ptr<BridgePKBuffer> AttachPKBufferToTargetScans(PhysicalOperator &op, const vector<string> &pk_columns,
                                                              const TableCatalogEntry &target,
                                                              shared_ptr<BridgePKBuffer> buffer = nullptr) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &scan = op.Cast<PhysicalTableScan>();
		if (auto *provider_data = dynamic_cast<ProviderScanFunctionData *>(scan.bind_data.get())) {
			if (provider_data->table == &target && provider_data->owned_stream_data) {
				if (!buffer) {
					buffer = make_shared_ptr<BridgePKBuffer>();
				}
				provider_data->owned_stream_data->include_pk_columns = pk_columns;
				provider_data->owned_stream_data->pk_buffer = buffer;
			}
		}
	}
	// No early return: every scan of the target has to be attached, not just the first one found.
	for (auto &child : op.children) {
		buffer = AttachPKBufferToTargetScans(child.get(), pk_columns, target, std::move(buffer));
	}
	return buffer;
}

} // namespace duckdb
