#pragma once

// The bridge and provider scan paths are the same machinery over two different global-state types:
// both project columns for the Arrow producer, both trail the primary key columns after the
// projected ones, and both hand DML row ids out of a shared BridgePKBuffer. The state types differ
// only in name, so the row-id half is a template rather than two copies.

#include "bridge_table_function.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

// `context` names the caller in the error message ("virtual_catalog" / "vcat_provider").
//
// `format` is the column's Arrow format string, or nullptr when the producer is DuckDB's own Arrow
// export and the layout is therefore known. It matters for strings: a DuckDB VARCHAR can arrive as
// utf8 (int32 offsets) or large_utf8 (int64 offsets), and reading one as the other silently returns
// garbage — so an unknown string layout is rejected rather than guessed at.
inline Value ReadArrowValue(ArrowArray &array, idx_t pos, const LogicalType &type, const char *format,
                            const char *context) {
	if (array.buffers[0]) {
		auto *validity = reinterpret_cast<const uint8_t *>(array.buffers[0]);
		auto bit_idx = array.offset + pos;
		if (!(validity[bit_idx / 8] & (1 << (bit_idx % 8)))) {
			return Value(type);
		}
	}

	auto offset = static_cast<idx_t>(array.offset) + pos;

	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return Value::TINYINT(reinterpret_cast<const int8_t *>(array.buffers[1])[offset]);
	case PhysicalType::INT16:
		return Value::SMALLINT(reinterpret_cast<const int16_t *>(array.buffers[1])[offset]);
	case PhysicalType::INT32:
		return Value::INTEGER(reinterpret_cast<const int32_t *>(array.buffers[1])[offset]);
	case PhysicalType::INT64:
		return Value::BIGINT(reinterpret_cast<const int64_t *>(array.buffers[1])[offset]);
	case PhysicalType::VARCHAR: {
		// Offsets in buffers[1], char data in buffers[2]; the offset WIDTH is what the format says.
		auto *data = reinterpret_cast<const char *>(array.buffers[2]);
		const bool is_utf8 = !format || (format[0] == 'u' && format[1] == '\0');
		const bool is_large_utf8 = format && format[0] == 'U' && format[1] == '\0';
		if (is_utf8) {
			auto *offsets = reinterpret_cast<const int32_t *>(array.buffers[1]);
			return Value(string(data + offsets[offset], NumericCast<size_t>(offsets[offset + 1] - offsets[offset])));
		}
		if (is_large_utf8) {
			auto *offsets = reinterpret_cast<const int64_t *>(array.buffers[1]);
			return Value(string(data + offsets[offset], NumericCast<size_t>(offsets[offset + 1] - offsets[offset])));
		}
		throw IOException("%s: primary key column uses the Arrow layout '%s', which cannot be read as a key; use "
		                  "utf8 or large_utf8",
		                  context, format);
	}
	default:
		throw IOException("%s: unsupported primary key column type: %s", context, type.ToString());
	}
}

// The row ids written into `rowid_vec` are indices into the PK buffer this call appends to -- the
// rest of the DML path resolves them back through that buffer, so they are manufactured here rather
// than coming from the source table.
template <class ScanGlobalState>
void AppendPKValuesAndEmitBufferRowIds(Vector &rowid_vec, ArrowArray &parent_array, ScanGlobalState &global_state,
                                       idx_t chunk_offset, idx_t count, const char *context) {
	auto row_ids = FlatVector::GetData<row_t>(rowid_vec);
	auto &buffer = *global_state.pk_buffer;

	for (idx_t i = 0; i < count; i++) {
		vector<Value> pk_vals;
		for (idx_t pk_idx = 0; pk_idx < global_state.pk_count; pk_idx++) {
			auto arrow_col = global_state.pk_start_index + pk_idx;
			auto &array = *parent_array.children[arrow_col];
			const char *format =
			    pk_idx < global_state.pk_formats.size() ? global_state.pk_formats[pk_idx].c_str() : nullptr;
			pk_vals.push_back(ReadArrowValue(array, chunk_offset + i, global_state.pk_types[pk_idx], format, context));
		}
		row_ids[i] = static_cast<row_t>(buffer.rows.size());
		buffer.rows.push_back(std::move(pk_vals));
	}
}

// ROW_ID is skipped: it is not a source column, so it must not reach the producer's projection.
inline unique_ptr<ArrowArrayStreamWrapper> BuildProjectionParamsAndProduceStream(const BridgeScanFunctionData &function,
                                                                                 const vector<column_t> &column_ids,
                                                                                 TableFilterSet *filters) {
	ArrowStreamParameters parameters;
	D_ASSERT(!column_ids.empty());
	auto &col_names = function.owned_stream_data->column_names;
	for (idx_t idx = 0; idx < column_ids.size(); idx++) {
		auto col_idx = column_ids[idx];
		// Bounds-checked, not just ROW_ID-checked: COLUMN_IDENTIFIER_EMPTY is also a sentinel far past
		// the end of col_names, and indexing the vector with either one reads out of bounds.
		if (col_idx != COLUMN_IDENTIFIER_ROW_ID && col_idx < col_names.size()) {
			parameters.projected_columns.projection_map[idx] = col_names[col_idx];
			parameters.projected_columns.columns.emplace_back(col_names[col_idx]);
			parameters.projected_columns.filter_to_col[idx] = col_idx;
		}
	}
	parameters.filters = filters;
	return function.scanner_producer(function.stream_factory_ptr, parameters);
}

// Depth-first for the first scan carrying bridge stream data; returns the buffer it just attached,
// or nullptr when the plan holds no such scan.
inline shared_ptr<BridgePKBuffer> FindScanAndAttachPKBuffer(PhysicalOperator &op, const vector<string> &pk_columns) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &scan = op.Cast<PhysicalTableScan>();
		auto *bridge_data = dynamic_cast<BridgeScanFunctionData *>(scan.bind_data.get());
		if (bridge_data && bridge_data->owned_stream_data) {
			bridge_data->owned_stream_data->include_pk_columns = pk_columns;
			auto buffer = make_shared_ptr<BridgePKBuffer>();
			bridge_data->owned_stream_data->pk_buffer = buffer;
			return buffer;
		}
	}
	for (auto &child : op.children) {
		auto result = FindScanAndAttachPKBuffer(child.get(), pk_columns);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

} // namespace duckdb
