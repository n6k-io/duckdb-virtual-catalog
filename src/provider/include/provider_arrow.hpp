#pragma once

// Arrow IPC is the whole provider ABI: names, types, primary keys and rows all come out of these
// decoders, and nothing else crosses the boundary.

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class ClientContext;
class Connection;

namespace vcat_provider {

// The Arrow schema metadata key carrying a comma-separated primary key list, set by the host on the
// schema it returns. Absent means the table declares no primary key.
extern const char *const PRIMARY_KEY_METADATA_KEY;

struct DecodedProviderSchema {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> primary_keys;
};

// Decode one Arrow IPC schema message (`pa.Schema.serialize()`). `what` names the caller in errors.
DecodedProviderSchema DecodeSchemaMessage(ClientContext &context, const string &ipc_bytes, const char *what);

// Every batch is decoded up front, since the provider contract returns one fully materialized
// table; `out` then hands them over one at a time.
void MakeStreamFromIpc(const string &ipc_bytes, ArrowArrayStream *out, const char *what);

struct TrailingColumns {
	vector<LogicalType> types;
	// Arrow format string per column: the DuckDB type does not pin the layout (a VARCHAR key may be
	// utf8 or large_utf8) and the key reader has to know which.
	vector<string> formats;
};

// `stream`'s trailing `count` columns, which is where the scan path appends the primary key columns
// it needs for row ids.
TrailingColumns DescribeTrailingColumns(ClientContext &context, ArrowArrayStream &stream, idx_t count,
                                        const char *what);

// Gathers loose Values into DataChunks: DML key rows come out of a BridgePKBuffer one value at a
// time, not as a relation, and the Arrow encoder takes chunks.
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

// One complete IPC stream — schema, batches, end-of-stream — for a write UDF to read back with
// `pyarrow.ipc.open_stream`. Empty `chunks` still produces a valid, empty stream.
string EncodeChunksAsIpc(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names,
                         const vector<unique_ptr<DataChunk>> &chunks);

// Calls `udf(table_name, <arrow blob>)`, plus `extra` bound after, and returns the row count it
// reports. Bound, never spliced: the payload is binary and the table name is the user's.
int64_t CallWriteUdf(Connection &conn, const string &udf_name, const string &table_name, const string &arrow_ipc,
                     const vector<Value> &extra, const char *what);

} // namespace vcat_provider
} // namespace duckdb
