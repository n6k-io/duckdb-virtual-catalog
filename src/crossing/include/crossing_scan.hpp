#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include "crossing.hpp"
#include "internal/source.hpp"

namespace duckdb {

class CrossingTableCatalogEntry;

static constexpr const char *CROSSING_SCAN_FUNCTION = "crossing_table_scan";

struct CrossingScanBindData : public TableFunctionData, public CrossingReadCarrier {
	optional_ptr<CrossingSource> source;
	string source_schema;
	string source_table;
	vector<string> column_names;
	vector<LogicalType> column_types;

	shared_ptr<CrossingFragment> fragment;

	optional_ptr<CrossingTableCatalogEntry> table;

	optional_ptr<CrossingFragment> GetReadFragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return *source.get_mutable();
	}

	//! The plan is bound against catalog entries this attach owns, and the source outlives neither.
	bool SupportStatementCache() const override {
		return false;
	}
};

//! What the source is told about one scan. The fragment outlives it: the bind data owns both.
class CrossingScanQuery : public CrossingReadQuery {
public:
	CrossingScanQuery(shared_ptr<CrossingFragment> fragment_p, vector<LogicalType> types_p)
	    : fragment(std::move(fragment_p)), types(std::move(types_p)) {
	}

	const LogicalOperator &Plan() const override;
	string ToString() const override;
	vector<CrossingTableUse> Tables() const override;
	CrossingVerb Kind() const override {
		return CrossingVerb::SELECT;
	}
	const vector<LogicalType> &Types() const override {
		return types;
	}

private:
	shared_ptr<CrossingFragment> fragment;
	vector<LogicalType> types;
};

struct CrossingScanGlobalState : public GlobalTableFunctionState {
	unique_ptr<CrossingScanQuery> query;
	unique_ptr<CrossingReader> reader;
	DataChunk source_chunk;
	vector<column_t> column_ids;
	//! Where each requested column sits in the chunk the fragment produces.
	vector<idx_t> source_position;
	idx_t source_column_count = 0;
	bool finished = false;

	idx_t rows_emitted = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

TableFunction CrossingScanFunction();

//! The bind data for a scan of `entry`, with its fragment built and its floor sealed.
unique_ptr<FunctionData> MakeCrossingScanBindData(CrossingTableCatalogEntry &entry, CrossingSource &source);

} // namespace duckdb
