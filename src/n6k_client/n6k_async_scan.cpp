// Async full-scan source operator — the payoff mechanism. A full `SELECT * FROM <n6k table>` normally
// binds to `n6k_scan` (ArrowTableFunction) whose blocking Arrow stream freezes the worker while it waits
// for network frames, so concurrent scans serialize. This module rewrites that LogicalGet (via an
// OptimizerExtension) into N6kAsyncScanSource: a custom PhysicalOperator source that drives the scan's
// RequestState frame-by-frame through N6kArrowFrameDecoder, and — when no frame is buffered — returns
// SourceResultType::BLOCKED with NO task so the worker goes fully idle. RequestState::SetOnFrame fires
// interrupt_state.Callback() when the next frame lands, rescheduling the parked pipeline. The
// BLOCKED-with-no-task + external-Callback wake was proven on a single wasm worker before this landed.
//
// Scope: any projection, and any filter set that renders exactly onto the wire. A filter that does
// not (see filter_json.hpp) declines the rewrite and stays on the blocking path, which renders it or
// throws. Both native and the coi/wasm_threads bundle take the async path.

#include "n6k_async_scan.hpp"

#include "filter_json.hpp"
#include "n6k_arrow_stream.hpp" // N6kLazyScanFunctionData + duckdb arrow scan headers
#include "n6k_catalog_session.hpp"
#include "n6k_frame_source.hpp" // N6kFrameWaker / N6kFrameSourceState / N6kDrainFrames
#include "n6k_protocol_generated.hpp"
#include "n6k_str_utils.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <atomic>
#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

constexpr bool kN6kAsyncScanEnabled = true;

namespace {

// Park/wake instrumentation surfaced by n6k_async_scan_stats(), process-wide across concurrent scans.
struct AsyncScanStats {
	std::atomic<int64_t> parks {0};
	std::atomic<int64_t> wakes {0};
};
AsyncScanStats &Stats() {
	static AsyncScanStats stats;
	return stats;
}

// The waker, the per-scan state and the drain live in n6k_frame_source.hpp so the pushed-aggregate
// source reuses them verbatim; only the park/wake counters below are specific to this operator.
struct N6kAsyncScanGlobalState : public N6kFrameSourceState {
	explicit N6kAsyncScanGlobalState(ClientContext &ctx) : N6kFrameSourceState(ctx) {
	}
	bool schema_checked = false;
};

// ArrowToDuckDB reads each batch's children positionally but looks arrow_convert_data up by BASE
// column id, so a server that ignored `columns=` would decode with mismatched types instead of
// failing. Checked once, against the first batch's schema.
void VerifyProjectionWidth(const N6kAsyncScanGlobalState &gs, idx_t expected, const string &catalog,
                           const string &schema, const string &table) {
	auto *decoded = gs.decoder.GetSchema();
	if (!decoded) {
		return;
	}
	auto got = static_cast<idx_t>(decoded->n_children);
	if (got != expected) {
		throw IOException("n6k[%s] scan %s.%s: server returned %llu columns, expected %llu", catalog, schema, table,
		                  static_cast<uint64_t>(got), static_cast<uint64_t>(expected));
	}
}

class N6kAsyncScanSource : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	N6kAsyncScanSource(PhysicalPlan &physical_plan, std::shared_ptr<FunctionData> bind_holder_p,
	                   std::shared_ptr<CatalogSession> session_p, string catalog_p, string schema_p, string table_p,
	                   string query_p, vector<column_t> column_ids_p, vector<LogicalType> types_p,
	                   idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      bind_holder(std::move(bind_holder_p)), session(std::move(session_p)), catalog(std::move(catalog_p)),
	      schema(std::move(schema_p)), table(std::move(table_p)), query(std::move(query_p)),
	      column_ids(std::move(column_ids_p)) {
	}

	std::shared_ptr<FunctionData> bind_holder; // keeps arrow_table (arrow_convert_data) alive
	std::shared_ptr<CatalogSession> session;
	string catalog;
	string schema;
	string table;
	// Scan request query string; empty = full scan (all columns, no filter).
	string query;
	vector<column_t> column_ids;

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		InsertionOrderPreservingMap<string> result;
		result["Table"] = schema + "." + table;
		result["Pushed"] = query.empty() ? "*" : query;
		return result;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		auto gs = make_uniq<N6kAsyncScanGlobalState>(context);
		gs->scan_state.column_ids = column_ids;
		return std::move(gs);
	}

	bool IsSource() const override {
		return true;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &gs = input.global_state.Cast<N6kAsyncScanGlobalState>();
		auto &data = bind_holder->Cast<N6kLazyScanFunctionData>();

		if (!gs.started) {
			gs.req = session->StartScanRequest(schema, table, query);
			gs.started = true;
			if (!gs.req) {
				throw InternalException("n6k async scan: session did not expose a RequestState");
			}
			auto waker = gs.waker;
			gs.req->SetOnFrame([waker]() {
				if (waker->RecordFrameAndWake()) {
					Stats().wakes.fetch_add(1, std::memory_order_relaxed);
				}
			});
		}

		for (;;) {
			if (gs.has_batch) {
				auto len = static_cast<idx_t>(gs.scan_state.chunk->arrow_array.length);
				if (gs.scan_state.chunk_offset >= len) {
					gs.has_batch = false;
				} else {
					idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, len - gs.scan_state.chunk_offset);
					chunk.SetCardinality(output_size);
					ArrowTableFunction::ArrowToDuckDB(gs.scan_state, data.arrow_table.GetColumns(), chunk);
					chunk.Verify();
					gs.scan_state.chunk_offset += chunk.size();
					return SourceResultType::HAVE_MORE_OUTPUT;
				}
			}

			if (gs.decoder.HasBatch()) {
				if (!gs.schema_checked) {
					VerifyProjectionWidth(gs, column_ids.size(), catalog, schema, table);
					gs.schema_checked = true;
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
				N6kThrowSourceError(gs, catalog, n6k::OP_SCAN, "scan"); // [[noreturn]]
			}
			if (gs.stream_done) {
				return SourceResultType::FINISHED;
			}

			uint64_t sampled = gs.waker->DeliveredFrameCount();
			N6kDrainFrames(gs);
			if (gs.decoder.HasBatch() || gs.stream_done || gs.saw_error) {
				continue;
			}
			if (gs.waker->ArmWakeupIfNoFrameSince(input.interrupt_state, sampled)) {
				Stats().parks.fetch_add(1, std::memory_order_relaxed);
				return SourceResultType::BLOCKED;
			}
			// A frame landed during the drain → re-drain instead of parking (no lost wakeup).
		}
	}
};

class N6kAsyncScanLogical : public LogicalExtensionOperator {
public:
	N6kAsyncScanLogical(std::shared_ptr<FunctionData> bind_holder_p, std::shared_ptr<CatalogSession> session_p,
	                    string catalog_p, string schema_p, string table_p, string query_p, idx_t table_index_p,
	                    vector<column_t> column_ids_p, vector<LogicalType> types_p, vector<ColumnBinding> bindings_p,
	                    idx_t cardinality_p)
	    : bind_holder(std::move(bind_holder_p)), session(std::move(session_p)), catalog(std::move(catalog_p)),
	      schema(std::move(schema_p)), table(std::move(table_p)), query(std::move(query_p)), table_index(table_index_p),
	      column_ids(std::move(column_ids_p)), bindings(std::move(bindings_p)), resolved_types(std::move(types_p)),
	      cardinality(cardinality_p) {
		types = resolved_types;
	}

	std::shared_ptr<FunctionData> bind_holder;
	std::shared_ptr<CatalogSession> session;
	string catalog;
	string schema;
	string table;
	string query;
	idx_t table_index;
	vector<column_t> column_ids;
	vector<ColumnBinding> bindings;
	// A private copy of the output types: LogicalOperator::ResolveOperatorTypes() clears `types` and calls
	// ResolveTypes() (some post-rewrite passes do), so ResolveTypes() must restore them from here or the
	// physical operator ends up with a 0-column output chunk.
	vector<LogicalType> resolved_types;
	idx_t cardinality;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<N6kAsyncScanSource>(bind_holder, session, catalog, schema, table, query, column_ids, types,
		                                        cardinality);
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}

	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bnd) override {
		bnd = bindings;
	}

	string GetExtensionName() const override {
		return "n6k_async_scan";
	}

protected:
	void ResolveTypes() override {
		types = resolved_types;
	}
};

// Raw Arrow column names, base-indexed — the names the server knows. Not get.names, which
// PopulateArrowTableSchema deduplicates and so may have renamed a column the server would not accept.
vector<string> ArrowColumnNames(const N6kLazyScanFunctionData &data) {
	const auto &arrow_schema = data.schema_root.arrow_schema;
	auto n = static_cast<idx_t>(arrow_schema.n_children);
	vector<string> names;
	names.reserve(n);
	for (idx_t i = 0; i < n; i++) {
		names.emplace_back(arrow_schema.children[i]->name);
	}
	return names;
}

// Decide whether this Get can run on the async source and, if so, produce the base column ids to
// decode and the scan query string. Declining leaves the plan on DuckDB's blocking arrow scan.
bool TryPlanAsyncScan(LogicalGet &get, const N6kLazyScanFunctionData &data, vector<column_t> &out_ids,
                      string &out_query) {
	if (!get.parameters.empty() || !get.projected_input.empty()) {
		return false; // table-in-out scan
	}
	const auto &ids = get.GetColumnIds();
	if (ids.empty()) {
		return false; // nothing to decode (COUNT(*)); the aggregate rule owns that shape
	}
	auto names = ArrowColumnNames(data);

	out_ids.clear();
	out_ids.reserve(ids.size());
	bool identity = ids.size() == names.size();
	for (idx_t i = 0; i < ids.size(); i++) {
		if (ids[i].IsRowIdColumn()) {
			return false;
		}
		auto base = ids[i].GetPrimaryIndex();
		if (base >= names.size()) {
			return false; // virtual column: nothing to ask the server for
		}
		identity = identity && base == i;
		out_ids.push_back(base);
	}

	// A full scan still asks for no columns, so its wire form is unchanged.
	out_query.clear();
	if (!identity) {
		out_query = "columns=";
		for (idx_t i = 0; i < out_ids.size(); i++) {
			if (i > 0) {
				out_query += ",";
			}
			out_query += n6k::UrlEncode(names[out_ids[i]]);
		}
	}

	// Pushed filters are authoritative — DuckDB does not re-apply them above an arrow scan — so one
	// that cannot be rendered exactly must decline rather than be dropped. And table_filters is keyed
	// by BASE column index, not by position in GetColumnIds(): mixing the two misnames a filter, which
	// comes back as wrong rows rather than an error.
	if (!get.table_filters.filters.empty()) {
		unordered_map<idx_t, idx_t> filter_to_col;
		for (auto &entry : get.table_filters.filters) {
			filter_to_col[entry.first] = entry.first;
		}
		auto options = n6k_filter_json::FilterJsonOptions::Wire();
		n6k_filter_json::FilterSerializeResult result;
		auto filters_json = n6k_filter_json::SerializeFilters(get.table_filters, filter_to_col, names, options, result);
		if (!result.all_exact) {
			return false;
		}
		if (!filters_json.empty()) {
			if (!out_query.empty()) {
				out_query += "&";
			}
			out_query += "filters=" + n6k::UrlEncode(filters_json);
		}
	}
	return true;
}

// Swap an accepted LogicalGet for the async source. Moves the bind data into the new operator so
// arrow_table (arrow_convert_data) stays alive; the LogicalGet is destroyed when `op` is reassigned.
void RewriteToAsyncScan(unique_ptr<LogicalOperator> &op, LogicalGet &get, N6kLazyScanFunctionData &data,
                        vector<column_t> column_ids, string query) {
	auto &lazy = *data.owned_stream_data;
	string catalog = data.table ? data.table->ParentCatalog().GetName() : string();
	auto session = lazy.session;
	auto schema = lazy.schema_name;
	auto table = lazy.table_name;
	auto table_index = get.table_index;
	auto types = get.types;
	auto bindings = get.GetColumnBindings();
	// get.bind_data is a duckdb unique_ptr; release into a std::shared_ptr (FunctionData has a virtual dtor).
	std::shared_ptr<FunctionData> held(get.bind_data.release());
	op = make_uniq<N6kAsyncScanLogical>(std::move(held), std::move(session), std::move(catalog), std::move(schema),
	                                    std::move(table), std::move(query), table_index, std::move(column_ids),
	                                    std::move(types), std::move(bindings), /*cardinality=*/1);
}

void ReplaceScanGetsInTree(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		// The catalog scan is registered with NO arguments (n6k_table_entry.cpp) and backed by
		// N6kLazyScanFunctionData (which owns the CatalogSession). Discriminating on name + arity
		// avoids RTTI/dynamic_cast, and the arity guard keeps the Cast below sound if another
		// "n6k_scan" registration is ever added.
		if (get.function.name == "n6k_scan" && get.function.arguments.empty() && get.bind_data) {
			auto &data = get.bind_data->Cast<N6kLazyScanFunctionData>();
			vector<column_t> column_ids;
			string query;
			if (data.owned_stream_data->session && TryPlanAsyncScan(get, data, column_ids, query)) {
				RewriteToAsyncScan(op, get, data, std::move(column_ids), std::move(query));
			}
			return;
		}
	}
	for (auto &child : op->children) {
		ReplaceScanGetsInTree(child);
	}
}

void OptimizeAsyncScan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!kN6kAsyncScanEnabled) {
		return;
	}
	ReplaceScanGetsInTree(plan);
}

struct N6kAsyncScanStatsBind : public TableFunctionData {};

struct N6kAsyncScanStatsState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> AsyncScanStatsBind(ClientContext &, TableFunctionBindInput &,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	names = {"parks", "wakes"};
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
	return make_uniq<N6kAsyncScanStatsBind>();
}

unique_ptr<GlobalTableFunctionState> AsyncScanStatsInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kAsyncScanStatsState>();
}

void AsyncScanStatsScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kAsyncScanStatsState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;
	output.SetValue(0, 0, Value::BIGINT(Stats().parks.load(std::memory_order_relaxed)));
	output.SetValue(1, 0, Value::BIGINT(Stats().wakes.load(std::memory_order_relaxed)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kAsyncScan(ExtensionLoader &loader) {
	TableFunction stats("n6k_async_scan_stats", {}, AsyncScanStatsScan, AsyncScanStatsBind, AsyncScanStatsInitGlobal);
	loader.RegisterFunction(stats);

	OptimizerExtension opt;
	opt.optimize_function = OptimizeAsyncScan;
	OptimizerExtension::Register(DBConfig::GetConfig(loader.GetDatabaseInstance()), opt);
}

} // namespace duckdb
