#include "n6k_exec.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_table_entry.hpp"

namespace duckdb {

struct N6kExecSourceState : public GlobalSourceState {
	bool done = false;
};

N6kExec::N6kExec(PhysicalPlan &physical_plan, N6kTableCatalogEntry &table_p, string sql_p, vector<LogicalType> types_p,
                 idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), sql(std::move(sql_p)) {
}

unique_ptr<GlobalSourceState> N6kExec::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<N6kExecSourceState>();
}

SourceResultType N6kExec::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                          OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<N6kExecSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	source_state.done = true;

	if (!table.session) {
		throw IOException("n6k: attached-catalog exec requires a WebSocket session; base_url=" + table.base_url);
	}
	auto affected = table.session->Exec(sql);

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(affected));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
