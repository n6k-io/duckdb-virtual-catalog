// Aggregate pushdown. An OptimizerExtension rewrites Aggregate(Get(n6k_scan)) into a source that
// asks the server for the grouped result (OP_AGGREGATE) instead of streaming every row into the
// client and grouping locally.
//
// Two facts shape the design:
//
//   1. OptimizerExtensions run last, after FILTER_PUSHDOWN, so predicates already live in
//      get.table_filters and the LogicalFilter is gone. A LogicalFilter that survives is one DuckDB
//      could NOT turn into a TableFilter -- an arbitrary expression -- so the shape to match is
//      Aggregate(Get) and nothing else.
//   2. The rule fires at plan time, and a rewritten plan has no way back. So the server's support
//      is read from the handshake capability rather than discovered from a failed request.
//
// Anything unrecognized leaves the plan untouched and falls back to today's row-streaming path.

#include "n6k_agg_pushdown.hpp"

#include "n6k_arrow_stream.hpp" // N6kLazyScanFunctionData
#include "n6k_body_builders.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_frame_source.hpp"
#include "n6k_protocol_generated.hpp"
#include "filter_json.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

constexpr bool kN6kAggPushdownEnabled = true;

namespace {

struct AggPushdownStats {
	std::atomic<int64_t> considered {0};
	std::atomic<int64_t> pushed {0};
	std::atomic<int64_t> rejected {0};
	std::atomic<int64_t> result_rows {0};
	std::atomic<int64_t> type_reconciles {0};
	std::mutex reason_mu;
	std::string last_reject_reason;
};

AggPushdownStats &GlobalAggPushdownStats() {
	static AggPushdownStats stats;
	return stats;
}

bool RecordRejectionAndFail(const string &reason) {
	auto &stats = GlobalAggPushdownStats();
	stats.rejected.fetch_add(1, std::memory_order_relaxed);
	std::lock_guard<std::mutex> lk(stats.reason_mu);
	stats.last_reject_reason = reason;
	return false;
}

struct AggSpec {
	std::shared_ptr<CatalogSession> session;
	string catalog;
	string schema;
	string table;
	vector<string> group_by;
	vector<string> referenced_columns;
	string filters_json;
	string aggregates_json;
};

// count(*) binds to `count_star` with no children; the rest take exactly one column reference.
const char *DuckDbAggToWireNameOrNull(const string &duckdb_name) {
	if (duckdb_name == "count_star") {
		return "count";
	}
	// String literals, not duckdb_name.c_str(): the result outlives this call at every call site.
	if (duckdb_name == "count") {
		return "count";
	}
	if (duckdb_name == "sum") {
		return "sum";
	}
	if (duckdb_name == "min") {
		return "min";
	}
	if (duckdb_name == "max") {
		return "max";
	}
	if (duckdb_name == "avg") {
		return "avg";
	}
	// Deliberately excludes sum_no_overflow: statistics propagation can rewrite sum() into it, and
	// its BIGINT return type is not what a plain sum() pushdown would produce.
	return nullptr;
}

bool IsPlainGroupBy(const LogicalAggregate &agg) {
	if (agg.grouping_sets.size() > 1) {
		return false;
	}
	if (agg.grouping_sets.empty()) {
		return true;
	}
	auto &only = agg.grouping_sets[0];
	if (only.size() != agg.groups.size()) {
		return false;
	}
	for (idx_t i = 0; i < agg.groups.size(); i++) {
		if (only.find(i) == only.end()) {
			return false;
		}
	}
	return true;
}

bool TryMatchPushdownableAggregate(LogicalOperator &op, AggSpec &out) {
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || op.children.size() != 1) {
		return false;
	}
	auto &agg = op.Cast<LogicalAggregate>();
	if (agg.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = agg.children[0]->Cast<LogicalGet>();

	// Same discrimination as the async-scan rule: the catalog scan is the no-argument registration,
	// and the arity guard keeps the Cast below sound.
	if (get.function.name != "n6k_scan" || !get.function.arguments.empty() || !get.bind_data) {
		return false;
	}
	auto &data = get.bind_data->Cast<N6kLazyScanFunctionData>();
	auto &lazy = *data.owned_stream_data;
	if (!lazy.session) {
		return false;
	}

	// Past this point the plan is a candidate, so refusals are worth counting.
	GlobalAggPushdownStats().considered.fetch_add(1, std::memory_order_relaxed);

	if (!lazy.session->HasCapability(n6k::CAP_AGGREGATE_PUSHDOWN)) {
		return RecordRejectionAndFail("server does not advertise aggregate_pushdown");
	}
	if (!get.parameters.empty() || !get.projected_input.empty()) {
		return RecordRejectionAndFail("table-in-out scan");
	}
	// Not groupings_index: the binder assigns that for every GROUP BY, so it says nothing about
	// ROLLUP/CUBE. Actual GROUPING() calls show up in grouping_functions, and ROLLUP/CUBE expand
	// grouping_sets beyond the single all-groups set. With grouping_functions empty, no column
	// binding references groupings_index, so GetTableIndex() need not report it.
	if (!agg.grouping_functions.empty() || !IsPlainGroupBy(agg)) {
		return RecordRejectionAndFail("ROLLUP / CUBE / GROUPING SETS");
	}

	auto &column_ids = get.GetColumnIds();

	// Group keys must be plain columns of this scan.
	for (auto &group : agg.groups) {
		if (group->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return RecordRejectionAndFail("group key is an expression, not a column");
		}
		auto &ref = group->Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index != get.table_index || ref.binding.column_index >= column_ids.size()) {
			return RecordRejectionAndFail("group key is not a column of the pushed table");
		}
		auto &name = get.GetColumnName(column_ids[ref.binding.column_index]);
		out.group_by.push_back(name);
		out.referenced_columns.push_back(name);
	}

	// Aggregates: a whitelisted function over at most one plain column.
	auto *doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	auto *arr = duckdb_yyjson::yyjson_mut_arr(doc);
	duckdb_yyjson::yyjson_mut_doc_set_root(doc, arr);
	auto bail = [&doc](const string &reason) {
		duckdb_yyjson::yyjson_mut_doc_free(doc);
		return RecordRejectionAndFail(reason);
	};

	for (auto &expr : agg.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return bail("non-aggregate expression in the aggregate list");
		}
		auto &fn = expr->Cast<BoundAggregateExpression>();
		if (fn.IsDistinct()) {
			return bail("DISTINCT aggregate");
		}
		if (fn.filter) {
			return bail("aggregate FILTER clause");
		}
		if (fn.order_bys) {
			return bail("ordered aggregate");
		}
		const char *wire = DuckDbAggToWireNameOrNull(fn.function.name);
		if (!wire) {
			return bail("unsupported aggregate '" + fn.function.name + "'");
		}

		auto *entry = duckdb_yyjson::yyjson_mut_obj(doc);
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(doc, entry, "fn", wire, std::strlen(wire));

		if (fn.function.name == "count_star") {
			if (!fn.children.empty()) {
				return bail("count_star with arguments");
			}
		} else {
			if (fn.children.size() != 1) {
				return bail("aggregate arity is not 1");
			}
			auto &child = *fn.children[0];
			if (child.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return bail("aggregate argument is an expression, not a column");
			}
			auto &ref = child.Cast<BoundColumnRefExpression>();
			if (ref.binding.table_index != get.table_index || ref.binding.column_index >= column_ids.size()) {
				return bail("aggregate argument is not a column of the pushed table");
			}
			auto &name = get.GetColumnName(column_ids[ref.binding.column_index]);
			duckdb_yyjson::yyjson_mut_obj_add_strncpy(doc, entry, "col", name.c_str(), name.size());
			out.referenced_columns.push_back(name);
		}
		duckdb_yyjson::yyjson_mut_arr_append(arr, entry);
	}

	// Filters must render exactly: once the server aggregates, a wrong row set is unrecoverable.
	//
	// A LogicalGet carries two different column keyings, and mixing them silently drops filters.
	// Column BINDINGS (the group keys and aggregate arguments above) index positions in
	// GetColumnIds(); table_filters keys are BASE column indices, i.e.
	// column_ids[binding.column_index].GetPrimaryIndex(). So names resolve through get.names
	// (base-indexed) with an identity map.
	if (!get.table_filters.filters.empty()) {
		const vector<string> &column_names = get.names;
		unordered_map<idx_t, idx_t> filter_to_col;
		for (auto &entry : get.table_filters.filters) {
			filter_to_col[entry.first] = entry.first;
		}
		auto options = n6k_filter_json::FilterJsonOptions::Wire();
		n6k_filter_json::FilterSerializeResult result;
		out.filters_json =
		    n6k_filter_json::SerializeFilters(get.table_filters, filter_to_col, column_names, options, result);
		if (!result.all_exact) {
			return bail("filter on '" + result.first_unsupported + "' has no wire representation");
		}
	}

	size_t len = 0;
	auto *json = duckdb_yyjson::yyjson_mut_write(doc, 0, &len);
	out.aggregates_json.assign(json, len);
	free(json);
	duckdb_yyjson::yyjson_mut_doc_free(doc);

	out.session = lazy.session;
	out.schema = lazy.schema_name;
	out.table = lazy.table_name;
	out.catalog = data.table ? data.table->ParentCatalog().GetName() : string();
	return true;
}

struct N6kAggregateGlobalState : public N6kFrameSourceState {
	explicit N6kAggregateGlobalState(ClientContext &ctx) : N6kFrameSourceState(ctx) {
	}

	ArrowTableSchema arrow_table;
	bool schema_ready = false;
	bool any_cast = false;
	vector<bool> needs_cast;
	unique_ptr<DataChunk> scratch; // wire-typed staging when a column needs reconciling
};

// DuckDB's aggregate return types do not all survive the Arrow round trip. Anything outside the
// table below is a contract break, not a cast to attempt -- reinterpreting a mismatched buffer is
// silent corruption, so the caller throws instead.
bool CastFromWireIsLossless(const LogicalType &wire, const LogicalType &planned) {
	if (planned.id() == LogicalTypeId::HUGEINT || planned.id() == LogicalTypeId::UHUGEINT) {
		// sum() over any integer/boolean, and min/max over (U)HUGEINT, arrive as decimal128(38,0).
		// HUGEINT and UHUGEINT share that encoding, which is why the PLANNED type decides.
		return wire.id() == LogicalTypeId::DECIMAL && DecimalType::GetWidth(wire) == 38 &&
		       DecimalType::GetScale(wire) == 0;
	}
	if (planned.id() == LogicalTypeId::UUID) {
		return wire.id() == LogicalTypeId::VARCHAR;
	}
	return false;
}

class N6kAggregateSource : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	N6kAggregateSource(PhysicalPlan &physical_plan, std::shared_ptr<CatalogSession> session_p, string catalog_p,
	                   string body_p, string describe_p, vector<LogicalType> types_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      session(std::move(session_p)), catalog(std::move(catalog_p)), body(std::move(body_p)),
	      describe(std::move(describe_p)) {
	}

	std::shared_ptr<CatalogSession> session;
	string catalog;
	string body;
	string describe;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		auto gs = make_uniq<N6kAggregateGlobalState>(context);
		vector<column_t> ids;
		ids.reserve(types.size());
		for (idx_t i = 0; i < types.size(); i++) {
			ids.push_back(i);
		}
		gs->scan_state.column_ids = std::move(ids);
		return std::move(gs);
	}

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Pushed"] = describe;
		return result;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &gs = input.global_state.Cast<N6kAggregateGlobalState>();

		if (!gs.started) {
			gs.req = session->StartOpRequest(n6k::OP_AGGREGATE, body);
			gs.started = true;
			if (!gs.req) {
				throw InternalException("n6k aggregate pushdown: session did not expose a RequestState");
			}
			auto waker = gs.waker;
			gs.req->SetOnFrame([waker]() { waker->RecordFrameAndWake(); });
		}

		for (;;) {
			if (gs.has_batch) {
				auto len = static_cast<idx_t>(gs.scan_state.chunk->arrow_array.length);
				if (gs.scan_state.chunk_offset >= len) {
					gs.has_batch = false;
				} else {
					idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, len - gs.scan_state.chunk_offset);
					EmitChunk(context, gs, chunk, output_size);
					gs.scan_state.chunk_offset += chunk.size();
					GlobalAggPushdownStats().result_rows.fetch_add(static_cast<int64_t>(chunk.size()),
					                                               std::memory_order_relaxed);
					return SourceResultType::HAVE_MORE_OUTPUT;
				}
			}

			if (gs.decoder.HasBatch()) {
				if (!gs.schema_ready) {
					throw IOException("n6k[" + catalog + "] aggregate: a batch arrived before the schema");
				}
				gs.scan_state.Reset();
				auto wrapper = make_shared_ptr<ArrowArrayWrapper>();
				gs.decoder.TryPopBatch(&wrapper->arrow_array);
				gs.scan_state.chunk = std::move(wrapper);
				gs.scan_state.chunk_offset = 0;
				gs.has_batch = true;
				continue;
			}

			if (gs.saw_error) {
				N6kThrowSourceError(gs, catalog, n6k::OP_AGGREGATE, "aggregate"); // [[noreturn]]
			}
			if (gs.stream_done) {
				return SourceResultType::FINISHED;
			}

			uint64_t sampled = gs.waker->DeliveredFrameCount();
			N6kDrainFrames(gs);
			if (!gs.schema_ready && gs.decoder.HasSchema()) {
				ResolveSchema(context.client, gs);
			}
			if (gs.decoder.HasBatch() || gs.stream_done || gs.saw_error) {
				continue;
			}
			if (gs.waker->ArmWakeupIfNoFrameSince(input.interrupt_state, sampled)) {
				return SourceResultType::BLOCKED;
			}
		}
	}

private:
	// The plan committed to a column layout before anything was sent; the server's RESP_SCHEMA is
	// the first chance to check that promise. Decide the per-column cast plan once, here.
	void ResolveSchema(ClientContext &context, N6kAggregateGlobalState &gs) const {
		ArrowTableFunction::PopulateArrowTableSchema(context, gs.arrow_table, *gs.decoder.GetSchema());
		auto wire_types = gs.arrow_table.GetTypes();
		if (wire_types.size() != types.size()) {
			throw IOException("n6k[" + catalog + "] aggregate: server returned " + std::to_string(wire_types.size()) +
			                  " columns, plan expects " + std::to_string(types.size()));
		}
		gs.needs_cast.resize(types.size(), false);
		for (idx_t i = 0; i < types.size(); i++) {
			if (wire_types[i] == types[i]) {
				continue;
			}
			if (!CastFromWireIsLossless(wire_types[i], types[i])) {
				throw IOException("n6k[" + catalog + "] aggregate: column " + std::to_string(i) + " arrived as " +
				                  wire_types[i].ToString() + ", plan expects " + types[i].ToString());
			}
			gs.needs_cast[i] = true;
			gs.any_cast = true;
			GlobalAggPushdownStats().type_reconciles.fetch_add(1, std::memory_order_relaxed);
		}
		if (gs.any_cast) {
			gs.scratch = make_uniq<DataChunk>();
			gs.scratch->Initialize(context, wire_types);
		}
		gs.schema_ready = true;
	}

	void EmitChunk(ExecutionContext &context, N6kAggregateGlobalState &gs, DataChunk &chunk, idx_t count) const {
		if (!gs.any_cast) {
			chunk.SetCardinality(count);
			ArrowTableFunction::ArrowToDuckDB(gs.scan_state, gs.arrow_table.GetColumns(), chunk);
			chunk.Verify();
			return;
		}
		// Convert into wire-typed staging, then cast only the columns that need it. There is no
		// DataChunk-level cast, so this is per column.
		gs.scratch->Reset();
		gs.scratch->SetCardinality(count);
		ArrowTableFunction::ArrowToDuckDB(gs.scan_state, gs.arrow_table.GetColumns(), *gs.scratch);
		chunk.SetCardinality(gs.scratch->size());
		for (idx_t i = 0; i < types.size(); i++) {
			if (gs.needs_cast[i]) {
				VectorOperations::Cast(context.client, gs.scratch->data[i], chunk.data[i], gs.scratch->size());
			} else {
				chunk.data[i].Reference(gs.scratch->data[i]);
			}
		}
		chunk.Verify();
	}
};

class N6kAggregateLogical : public LogicalExtensionOperator {
public:
	N6kAggregateLogical(std::shared_ptr<CatalogSession> session_p, string catalog_p, string body_p, string describe_p,
	                    idx_t group_index_p, idx_t aggregate_index_p, vector<LogicalType> types_p,
	                    vector<ColumnBinding> bindings_p, idx_t cardinality_p)
	    : session(std::move(session_p)), catalog(std::move(catalog_p)), body(std::move(body_p)),
	      describe(std::move(describe_p)), group_index(group_index_p), aggregate_index(aggregate_index_p),
	      bindings(std::move(bindings_p)), resolved_types(std::move(types_p)), cardinality(cardinality_p) {
		types = resolved_types;
	}

	std::shared_ptr<CatalogSession> session;
	string catalog;
	string body;
	string describe;
	idx_t group_index;
	idx_t aggregate_index;
	vector<ColumnBinding> bindings;
	// LogicalOperator::ResolveOperatorTypes() clears `types` and calls ResolveTypes(), so keep a
	// private copy to restore from or the physical operator ends up with a 0-column output chunk.
	vector<LogicalType> resolved_types;
	idx_t cardinality;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<N6kAggregateSource>(session, catalog, body, describe, types, cardinality);
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}

	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bnd) override {
		bnd = bindings;
	}

	vector<idx_t> GetTableIndex() const override {
		return {group_index, aggregate_index};
	}

	string GetExtensionName() const override {
		return "n6k_aggregate";
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Pushed"] = describe;
		return result;
	}

protected:
	void ResolveTypes() override {
		types = resolved_types;
	}
};

string RenderPushedAggregateForExplain(const AggSpec &spec) {
	string out = spec.schema + "." + spec.table;
	if (!spec.group_by.empty()) {
		out += " GROUP BY ";
		for (idx_t i = 0; i < spec.group_by.size(); i++) {
			out += (i ? ", " : "") + spec.group_by[i];
		}
	}
	if (!spec.filters_json.empty()) {
		out += " FILTERED";
	}
	return out;
}

void ReplacePushdownableAggregatesInTree(unique_ptr<LogicalOperator> &op) {
	AggSpec spec;
	if (TryMatchPushdownableAggregate(*op, spec)) {
		auto &agg = op->Cast<LogicalAggregate>();
		// Snapshot everything before reassigning `op` — that destroys the aggregate and its child.
		auto types = agg.types;
		auto bindings = agg.GetColumnBindings();
		auto group_index = agg.group_index;
		auto aggregate_index = agg.aggregate_index;
		auto body = n6k_body::Aggregate(spec.schema, spec.table, spec.referenced_columns, spec.filters_json,
		                                spec.group_by, spec.aggregates_json);
		auto describe = RenderPushedAggregateForExplain(spec);
		op = make_uniq<N6kAggregateLogical>(std::move(spec.session), std::move(spec.catalog), std::move(body),
		                                    std::move(describe), group_index, aggregate_index, std::move(types),
		                                    std::move(bindings), /*cardinality=*/1);
		GlobalAggPushdownStats().pushed.fetch_add(1, std::memory_order_relaxed);
		return;
	}
	for (auto &child : op->children) {
		ReplacePushdownableAggregatesInTree(child);
	}
}

void OptimizeAggPushdown(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!kN6kAggPushdownEnabled) {
		return;
	}
	ReplacePushdownableAggregatesInTree(plan);
}

struct StatsBindData : public TableFunctionData {};

struct StatsState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> AggPushdownStatsBind(ClientContext &, TableFunctionBindInput &,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names = {"considered", "pushed", "rejected", "result_rows", "type_reconciles", "last_reject_reason"};
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR};
	return make_uniq<StatsBindData>();
}

unique_ptr<GlobalTableFunctionState> AggPushdownStatsInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<StatsState>();
}

void AggPushdownStatsScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<StatsState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;
	auto &stats = GlobalAggPushdownStats();
	string reason;
	{
		std::lock_guard<std::mutex> lk(stats.reason_mu);
		reason = stats.last_reject_reason;
	}
	output.SetValue(0, 0, Value::BIGINT(stats.considered.load(std::memory_order_relaxed)));
	output.SetValue(1, 0, Value::BIGINT(stats.pushed.load(std::memory_order_relaxed)));
	output.SetValue(2, 0, Value::BIGINT(stats.rejected.load(std::memory_order_relaxed)));
	output.SetValue(3, 0, Value::BIGINT(stats.result_rows.load(std::memory_order_relaxed)));
	output.SetValue(4, 0, Value::BIGINT(stats.type_reconciles.load(std::memory_order_relaxed)));
	output.SetValue(5, 0, Value(reason));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kAggregatePushdown(ExtensionLoader &loader) {
	TableFunction stats("n6k_agg_pushdown_stats", {}, AggPushdownStatsScan, AggPushdownStatsBind,
	                    AggPushdownStatsInitGlobal);
	loader.RegisterFunction(stats);

	OptimizerExtension opt;
	opt.optimize_function = OptimizeAggPushdown;
	OptimizerExtension::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()), opt);
}

} // namespace duckdb
