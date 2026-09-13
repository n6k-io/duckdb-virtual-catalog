#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "crossing.hpp"
#include "crossing_table_entry.hpp"
#include "internal/fragment.hpp"
#include "internal/source.hpp"

namespace duckdb {

class CrossingWriteCatalog : public DuckCatalog {
public:
	explicit CrossingWriteCatalog(AttachedDatabase &db);
	~CrossingWriteCatalog() override;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;
};

CrossingSeam SeamOf(CrossingTableCatalogEntry &table, CrossingVerb verb, vector<string> set_columns);

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb);

void RequireKey(CrossingTableCatalogEntry &table, const char *what);

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam);

class CrossingSeamEntry : public TableCatalogEntry {
public:
	CrossingSeamEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                  CrossingTableCatalogEntry &target, CrossingVerb verb, CrossingSeam seam,
	                  shared_ptr<CrossingFragment> fragment, string obstacle);

	CrossingTableCatalogEntry &target;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;
	string obstacle;
	vector<idx_t> key_positions;

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

static constexpr const char *CROSSING_SEAM_ENTRIES_KEY = "vcat_v2_seam_entries";

class CrossingSeamEntries : public ClientContextState {
public:
	void Hold(shared_ptr<CrossingSeamEntry> entry) {
		live.push_back(std::move(entry));
	}

	shared_ptr<CrossingSeamEntry> Share(CrossingSeamEntry &entry) {
		for (auto &held : live) {
			if (held.get() == &entry) {
				return held;
			}
		}
		throw InternalException("virtual_catalog_bridge: the seam of '%s' outlived its statement", entry.name);
	}

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		live.clear();
	}

	static shared_ptr<CrossingSeamEntries> Get(ClientContext &context) {
		return context.registered_state->GetOrCreate<CrossingSeamEntries>(CROSSING_SEAM_ENTRIES_KEY);
	}

private:
	vector<shared_ptr<CrossingSeamEntry>> live;
};

static constexpr const char *CROSSING_WRITE_FUNCTION = "crossing_table_write";

struct CrossingWriteBindData : public TableFunctionData, public CrossingWriteCarrier {
	optional_ptr<CrossingTableCatalogEntry> table;
	CrossingVerb verb = CrossingVerb::INSERT;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;

	optional_ptr<CrossingFragment> GetWriteFragment() override {
		return fragment.get();
	}
	CrossingSource &Source() override {
		return table->Source();
	}
	bool SupportStatementCache() const override {
		return false;
	}
};

TableFunction CrossingWriteFunction();

class CrossingWritePlanQuery : public CrossingWriteQuery {
public:
	CrossingWritePlanQuery(CrossingVerb verb_p, CrossingTableCatalogEntry &table_p, CrossingSeam seam_p,
	                       vector<LogicalType> types_p, const LogicalOperator &plan_p)
	    : verb(verb_p), table(table_p), seam(std::move(seam_p)), types(std::move(types_p)), plan(plan_p) {
	}

	const LogicalOperator &Plan() const override {
		return plan;
	}
	string ToString() const override;
	vector<CrossingTableUse> Tables() const override;
	CrossingVerb Kind() const override {
		return verb;
	}
	const vector<string> &SetColumns() const override {
		return seam.set_columns;
	}
	const vector<LogicalType> &Types() const override {
		return types;
	}

private:
	CrossingVerb verb;
	CrossingTableCatalogEntry &table;
	CrossingSeam seam;
	vector<LogicalType> types;
	const LogicalOperator &plan;
};

idx_t RunWrite(ClientContext &context, CrossingTableCatalogEntry &table, CrossingVerb verb, const CrossingSeam &seam,
               CrossingFragment &fragment, unique_ptr<ColumnDataCollection> rows);

void WidenKeyedWritesForReturning(LogicalOperator &plan);

struct CrossingWriteState : public GlobalSinkState {
	unique_ptr<ColumnDataCollection> rows;
	unordered_set<hash_t> seen_keys;
	unique_ptr<ColumnDataCollection> returned;
	ColumnDataScanState returned_scan;
	bool returned_scanning = false;
	idx_t affected_rows = 0;

	void SeeKey(DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index);
};

class CrossingWrite : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	CrossingWrite(PhysicalPlan &physical_plan, CrossingTableCatalogEntry &table, CrossingVerb verb, CrossingSeam seam,
	              shared_ptr<CrossingFragment> fragment, vector<LogicalType> types, idx_t estimated_cardinality);

	CrossingTableCatalogEntry &table;
	CrossingVerb verb;
	CrossingSeam seam;
	shared_ptr<CrossingFragment> fragment;
	shared_ptr<CrossingSeamEntry> entry;
	bool return_chunk = false;
	string obstacle;

	vector<unique_ptr<Expression>> seam_row;
	vector<unique_ptr<Expression>> returned_row;
	vector<idx_t> key_positions;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
