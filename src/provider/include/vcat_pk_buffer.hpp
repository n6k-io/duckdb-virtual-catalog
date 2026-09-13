#pragma once

#include "duckdb.hpp"

#include "vcat_key_check.hpp"

namespace duckdb {

// FIXME: unbounded — rows accumulates every PK value the statement scans; large deletes/updates
// could OOM.
struct BridgePKBuffer {
	// Guards Append alone. PKBufferRow reads unlocked because the DML operators call it from
	// Finalize, by which point every scan feeding the buffer has drained.
	mutex lock;
	vector<vector<Value>> rows;

	// Returns the index the row landed at, which becomes its manufactured row id. Allocating the id
	// here rather than from a per-scan counter is what lets several scans of one table share a
	// buffer without their ids colliding.
	idx_t Append(vector<Value> row) {
		lock_guard<mutex> guard(lock);
		rows.push_back(std::move(row));
		return rows.size() - 1;
	}
};

// A row id is an index into `rows` and carries no other identity, so the wrong buffer looks like
// the right one until the index misses. Every DML path resolves keys through here to make that miss
// an error rather than a write against whatever row the index landed on. Not left to
// duckdb::vector, whose bounds check is compiled out under DUCKDB_DEBUG_NO_SAFETY.
inline vector<Value> &PKBufferRow(BridgePKBuffer *buffer, int64_t row_id, idx_t key_columns, const char *context) {
	if (!buffer) {
		throw InternalException("%s: no primary key buffer was attached to the scan", context);
	}
	if (row_id < 0 || static_cast<idx_t>(row_id) >= buffer->rows.size()) {
		throw InternalException("%s: row id %lld is out of range for a key buffer holding %llu rows", context,
		                        static_cast<int64_t>(row_id), static_cast<uint64_t>(buffer->rows.size()));
	}
	auto &row = buffer->rows[static_cast<idx_t>(row_id)];
	if (row.size() != key_columns) {
		throw InternalException("%s: key buffer row %lld holds %llu columns, expected %llu", context,
		                        static_cast<int64_t>(row_id), static_cast<uint64_t>(row.size()),
		                        static_cast<uint64_t>(key_columns));
	}
	return row;
}

} // namespace duckdb
