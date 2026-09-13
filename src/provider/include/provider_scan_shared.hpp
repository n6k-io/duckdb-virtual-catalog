#pragma once

// The provider scan path. Arrow IPC is the wire format because the payload crosses a SQL boundary
// as a BLOB.

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/function/table/arrow.hpp"

#include <cstdlib>
#include <cstring>

#include "vcat_pk_buffer.hpp"

namespace duckdb {

struct ProviderStreamData {
	// The instance the scan UDF is registered on, held so it outlives the query below.
	shared_ptr<DatabaseInstance> source_db;
	// The scan UDF name.
	string table_name;
	// Virtual table name passed as the first UDF argument.
	string provider_table_name;
	vector<string> column_names;
	bool consumed;
	vector<string> include_pk_columns;
	shared_ptr<BridgePKBuffer> pk_buffer;
	vector<LogicalType> pk_types;
	// Arrow format string per primary key column: a provider's VARCHAR may arrive as utf8 or
	// large_utf8, and reading one as the other silently returns garbage.
	vector<string> pk_formats;
	unique_ptr<ParsedExpression> read_policy;

	// Connection must outlive the query result.
	unique_ptr<Connection> source_conn;

	ProviderStreamData() : consumed(false) {
	}
};

struct ProviderScanFunctionData : public ArrowScanFunctionData {
	unique_ptr<ProviderStreamData> owned_stream_data;
	unique_ptr<ResultArrowArrayStreamWrapper> owned_schema_wrapper;
	unique_ptr<Connection> schema_conn;
	TableCatalogEntry *table = nullptr;

	ProviderScanFunctionData(stream_factory_produce_t producer, unique_ptr<ProviderStreamData> stream_data)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)) {
	}

	~ProviderScanFunctionData() override {
		// owned_schema_wrapper aliases schema_root.arrow_schema's memory; null out release to avoid double-free.
		if (owned_schema_wrapper) {
			schema_root.arrow_schema.release = nullptr;
		}
	}

	bool SupportStatementCache() const override {
		return false;
	}
};

// `context` names the caller in the error message. `format` is the column's Arrow format string, or
// nullptr when the layout is known: utf8 (int32 offsets) and large_utf8 (int64 offsets) are
// indistinguishable from the buffer alone, so an unknown string layout is rejected, not guessed at.
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

	// InternalType() is the DuckDB layout, not the Arrow one: DECIMAL(9,2) reports INT32 while the
	// Arrow buffer is 16 bytes per element, and reading it at the INT32 stride returns a value from
	// the wrong row. The format string decides wherever the two widths can disagree.
	if (format && format[0] == 'd') {
		if (type.id() != LogicalTypeId::DECIMAL) {
			throw IOException("%s: primary key column has Arrow layout '%s' but DuckDB type %s", context, format,
			                  type.ToString());
		}
		// "d:precision,scale" or "d:precision,scale,bitWidth"; 128 bits when the width is omitted.
		int64_t bit_width = 128;
		if (auto *last_comma = strrchr(format, ',')) {
			if (strchr(format, ',') != last_comma) {
				char *width_end = nullptr;
				bit_width = std::strtol(last_comma + 1, &width_end, 10);
				// An unreadable width must not silently read as 0 and then as "not 128": the message
				// below would name a width the producer never sent.
				if (width_end == last_comma + 1 || *width_end != '\0') {
					throw IOException("%s: primary key column has an unreadable decimal width in Arrow layout '%s'",
					                  context, format);
				}
			}
		}
		if (bit_width != 128) {
			throw IOException("%s: primary key column uses %d-bit decimal, which cannot be read as a key; use "
			                  "decimal128",
			                  context, bit_width);
		}
		hugeint_t raw;
		memcpy(&raw, reinterpret_cast<const uint8_t *>(array.buffers[1]) + offset * 16, 16);
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(NumericCast<int16_t>(Hugeint::Cast<int64_t>(raw)), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(NumericCast<int32_t>(Hugeint::Cast<int64_t>(raw)), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(Hugeint::Cast<int64_t>(raw), width, scale);
		default:
			return Value::DECIMAL(raw, width, scale);
		}
	}

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

// The row ids written to `rowid_vec` are indices into the PK buffer this call appends to.
template <class ScanGlobalState>
void AppendPKValuesAndEmitBufferRowIds(Vector &rowid_vec, ArrowArray &parent_array, ScanGlobalState &global_state,
                                       idx_t chunk_offset, idx_t count, const char *context) {
	auto row_ids = FlatVector::GetData<row_t>(rowid_vec);
	auto &buffer = *global_state.pk_buffer;

	// The scan's arity check should have made this unreachable; it stays because the alternative to
	// throwing here is reading children[] past the end of a batch the host controls.
	auto needed = global_state.pk_start_index + global_state.pk_count;
	if (needed > NumericCast<idx_t>(parent_array.n_children) || global_state.pk_types.size() < global_state.pk_count) {
		throw InvalidInputException("%s: batch carries %lld columns, too few for the %llu key columns at offset %llu",
		                            context, static_cast<int64_t>(parent_array.n_children),
		                            static_cast<uint64_t>(global_state.pk_count),
		                            static_cast<uint64_t>(global_state.pk_start_index));
	}

	for (idx_t i = 0; i < count; i++) {
		vector<Value> pk_vals;
		for (idx_t pk_idx = 0; pk_idx < global_state.pk_count; pk_idx++) {
			auto arrow_col = global_state.pk_start_index + pk_idx;
			auto &array = *parent_array.children[arrow_col];
			const char *format =
			    pk_idx < global_state.pk_formats.size() ? global_state.pk_formats[pk_idx].c_str() : nullptr;
			pk_vals.push_back(ReadArrowValue(array, chunk_offset + i, global_state.pk_types[pk_idx], format, context));
		}
		row_ids[i] = static_cast<row_t>(buffer.Append(std::move(pk_vals)));
	}
}

inline unique_ptr<ArrowArrayStreamWrapper>
BuildProjectionParamsAndProduceStream(const ProviderScanFunctionData &function, const vector<column_t> &column_ids,
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

} // namespace duckdb
