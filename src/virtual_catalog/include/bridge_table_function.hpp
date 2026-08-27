#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "bridge_bridge.hpp"

namespace duckdb {

// FIXME: unbounded — rows accumulates all PK values for the whole scan; large deletes/updates could OOM.
struct BridgePKBuffer {
	vector<vector<Value>> rows;
};

struct BridgeStreamData {
	shared_ptr<BridgeInfo> bridge_info;
	// Fully-qualified source table name (bridge/native), or the scan UDF name (provider).
	string table_name;
	// Virtual table name passed as the first UDF argument; empty for bridge/native scans.
	string provider_table_name;
	vector<string> column_names;
	bool consumed;
	vector<string> include_pk_columns;
	shared_ptr<BridgePKBuffer> pk_buffer;
	vector<LogicalType> pk_types;
	// Arrow format string per primary key column, when the producer knows it. The provider path
	// fills this because its Arrow comes from the provider itself and a VARCHAR key may arrive as
	// utf8 OR large_utf8; the bridge path leaves it empty, its Arrow being DuckDB's own export.
	vector<string> pk_formats;

	// Connection must outlive the query result.
	unique_ptr<Connection> source_conn;

	BridgeStreamData() : consumed(false) {
	}
};

struct BridgeScanFunctionData : public ArrowScanFunctionData {
	unique_ptr<BridgeStreamData> owned_stream_data;
	// Keeps Arrow schema memory alive for read-only scans (BridgeBind path).
	unique_ptr<ResultArrowArrayStreamWrapper> owned_schema_wrapper;
	unique_ptr<Connection> schema_conn;
	// Back-pointer for get_bind_info: marks this scan as belonging to a base table (needed for UPDATE/DELETE).
	TableCatalogEntry *table = nullptr;

	BridgeScanFunctionData(stream_factory_produce_t producer, unique_ptr<BridgeStreamData> stream_data)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)) {
	}

	~BridgeScanFunctionData() override {
		// owned_schema_wrapper aliases schema_root.arrow_schema's memory; null out release to avoid double-free.
		if (owned_schema_wrapper) {
			schema_root.arrow_schema.release = nullptr;
		}
	}

	bool SupportStatementCache() const override {
		return false;
	}
};

void RegisterBridgeTableFunction(ExtensionLoader &loader);

} // namespace duckdb
