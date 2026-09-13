#include "n6k_capabilities_function.hpp"
#include "n6k_catalog.hpp"
#include "n6k_protocol_generated.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {

struct N6kCapabilitiesBind : public TableFunctionData {
	vector<string> capabilities;
};

struct N6kCapabilitiesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto &catalog = Catalog::GetCatalog(context, db_name);
	auto &n6k_catalog = catalog.Cast<N6kCatalog>();
	if (!n6k_catalog.session) {
		throw IOException("n6k: catalog '%s' has no active session", db_name);
	}

	auto result = make_uniq<N6kCapabilitiesBind>();
	for (auto &name : {n6k::CAP_AGGREGATE_PUSHDOWN}) {
		if (n6k_catalog.session->HasCapability(name)) {
			result->capabilities.emplace_back(name);
		}
	}

	names = {"capability"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kCapabilitiesState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kCapabilitiesBind>();
	auto &state = data_p.global_state->Cast<N6kCapabilitiesState>();

	idx_t count = 0;
	while (state.offset < bind_data.capabilities.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(bind_data.capabilities[state.offset]));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kCapabilities(ExtensionLoader &loader) {
	TableFunction fn("n6k_capabilities", {LogicalType::VARCHAR}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
