#pragma once

// Arrow IPC is the whole provider ABI: the schema and scan UDFs hand back bytes pyarrow wrote, and
// these turn them into what DuckDB needs. Nothing else crosses the boundary — no pipe-delimited type
// strings, no temp tables — so column names, types, primary keys and rows all come from one decoder.

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class ClientContext;
class Connection;

namespace vcat_provider {

// The Arrow schema metadata key carrying a comma-separated primary key list, set by the Python side
// (PK_METADATA_KEY in provider.py). Absent means the table declares no primary key.
extern const char *const PRIMARY_KEY_METADATA_KEY;

struct DecodedProviderSchema {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> primary_keys;
};

// Decode one Arrow IPC schema message (`pa.Schema.serialize()`). `what` names the caller in errors.
DecodedProviderSchema DecodeSchemaMessage(ClientContext &context, const string &ipc_bytes, const char *what);

// Decode a whole Arrow IPC stream (schema + batches) into a released-on-drop ArrowArrayStream. Every
// batch is decoded up front — the provider contract returns one fully materialized table, so there is
// nothing to stream from — and `out` then hands them over one at a time.
void MakeStreamFromIpc(const string &ipc_bytes, ArrowArrayStream *out, const char *what);

struct TrailingColumns {
	vector<LogicalType> types;
	// The Arrow format string per column. Kept because the DuckDB type does not pin the layout — a
	// VARCHAR key may be utf8 or large_utf8 — and the key reader has to know which.
	vector<string> formats;
};

// `stream`'s trailing `count` columns, which is where the scan path appends the primary key columns
// it needs for row ids.
TrailingColumns DescribeTrailingColumns(ClientContext &context, ArrowArrayStream &stream, idx_t count,
                                        const char *what);

// Gathers loose Values into DataChunks, so rows assembled one value at a time — DML key rows come out
// of a BridgePKBuffer, not a relation — can reach the Arrow encoder. Deliberately the same
// BeginRow/Append/EndRow shape as DuckDB's Appender, which is what this replaced.
class RowChunkBuilder {
public:
	explicit RowChunkBuilder(vector<LogicalType> types);

	void BeginRow();
	void Append(const Value &value);
	// Throws if the row did not supply exactly one value per column; `what` names the caller.
	void EndRow(const char *what);
	// Flushes the partial chunk and hands over everything built. The builder is spent afterwards.
	vector<unique_ptr<DataChunk>> Finish();

private:
	void Flush();

	vector<LogicalType> types_;
	vector<unique_ptr<DataChunk>> chunks_;
	unique_ptr<DataChunk> current_;
	idx_t row_ = 0;
	idx_t column_ = 0;
};

// Encode `chunks` as one complete Arrow IPC stream — schema, batches, end-of-stream — for a write UDF
// to read back with `pyarrow.ipc.open_stream`. `names`/`types` describe the columns; an empty
// `chunks` still produces a valid, empty stream.
string EncodeChunksAsIpc(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
                         const vector<unique_ptr<DataChunk>> &chunks);

// Call a provider write UDF as `udf(table_name, <arrow blob>)` (plus `extra` bound after, if given)
// and return the row count it reports. Bound, never spliced: the payload is binary and the table name
// is the user's. `what` names the operation in errors.
int64_t CallWriteUdf(Connection &conn, const string &udf_name, const string &table_name, const string &arrow_ipc,
                     const vector<Value> &extra, const char *what);

} // namespace vcat_provider
} // namespace duckdb
