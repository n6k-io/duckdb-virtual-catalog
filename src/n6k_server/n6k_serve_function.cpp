#include "n6k_serve_function.hpp"

#include "serve_bind_common.hpp"
#include "serve_reactor.hpp"
#include "uds_transport.hpp"

#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <cstdlib>
#include <limits>
#include <string>

namespace duckdb {

static constexpr const char *SOCKET_ENV = "N6K_DB_SOCKET";
static constexpr const char *FN_NAME = "n6k_serve_socket";
static constexpr const char *FD_FN_NAME = "n6k_serve_fd";
static constexpr const char *PUSH_FN_NAME = "n6k_serve_push_invalidate";
static constexpr const char *STATS_FN_NAME = "n6k_serve_stats";
// Verifier picked up automatically when the host defines one and names none. Registering the
// function is the whole opt-in, so the host never has to say twice that it wants auth.
static constexpr const char *DEFAULT_AUTH_FN_NAME = "n6k_authorize";

namespace {

struct N6kServeBindData : public TableFunctionData {
	vector<string> catalogs;
	// How the connection is obtained: dial `socket_path`, or adopt `fd` when it is non-negative.
	// `socket_path` doubles as the label reported in the result row.
	string socket_path;
	int fd = -1;
	// Keepalive override; < 0 means "leave it to $N6K_PING_INTERVAL".
	int64_t ping_interval_ms = -1;
	// Scalar function of (token, catalog) -> BOOLEAN gating every session; empty disables auth.
	string auth_function;
};

struct N6kServeState : public GlobalTableFunctionState {
	bool done = false;
};

struct N6kPushInvalidateBindData : public TableFunctionData {
	string catalog;
	vector<string> schemas;
};

} // namespace

static void N6kServeReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"connected", "requests_handled", "socket"};
	return_types = {LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::VARCHAR};
}

// Whether `name` resolves to a function on this database. Asked on a separate Connection, the same
// way the reactor later calls the verifier: a Python UDF is registered against the DatabaseInstance
// rather than the connection that created it, so it is visible from either.
//
// Naming a function that does not exist is not a no-op -- an unresolvable verifier refuses every
// session -- so auto-detection has to be a real lookup rather than an assumption.
static bool ServeFunctionIsDefined(ClientContext &context, const char *name) {
	try {
		Connection conn(DatabaseInstance::GetDatabase(context));
		auto stmt = conn.Prepare("SELECT count(*) FROM duckdb_functions() WHERE function_name = ?");
		if (stmt->HasError()) {
			return false;
		}
		vector<Value> params;
		params.emplace_back(string(name));
		auto result = stmt->Execute(params);
		if (result->HasError()) {
			return false;
		}
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return false;
		}
		const auto count = chunk->GetValue(0, 0);
		return !count.IsNull() && count.GetValue<int64_t>() > 0;
	} catch (const std::exception &) {
		return false;
	}
}

static unique_ptr<FunctionData> N6kServeBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	N6kServeReturnSchema(return_types, names);

	auto catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 0, FN_NAME);

	const char *socket_path = std::getenv(SOCKET_ENV);
	if (socket_path == nullptr || socket_path[0] == '\0') {
		throw BinderException("%s: %s is not set; it must name the Unix domain socket to serve on", FN_NAME,
		                      SOCKET_ENV);
	}

	auto result = make_uniq<N6kServeBindData>();
	result->catalogs = std::move(catalogs);
	result->socket_path = string(socket_path);
	return std::move(result);
}

// Same server, same result row; the fd arrives as an argument rather than through the environment.
// That is what lets one process serve several connections at once — N6K_DB_SOCKET is process-global,
// so the dial form cannot.
static unique_ptr<FunctionData> N6kServeFdBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	N6kServeReturnSchema(return_types, names);

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires a file descriptor: CALL %s(7)", FD_FN_NAME, FD_FN_NAME);
	}
	auto fd = input.inputs[0].GetValue<int64_t>();
	if (fd < 0 || fd > static_cast<int64_t>(std::numeric_limits<int>::max())) {
		throw BinderException("%s: %s is not a valid file descriptor", FD_FN_NAME, std::to_string(fd));
	}

	auto result = make_uniq<N6kServeBindData>();
	result->catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 1, FD_FN_NAME);
	result->fd = static_cast<int>(fd);
	result->socket_path = "fd:" + std::to_string(fd);

	auto ping = input.named_parameters.find("ping_interval_ms");
	if (ping != input.named_parameters.end() && !ping->second.IsNull()) {
		result->ping_interval_ms = ping->second.GetValue<int64_t>();
	}
	// Named explicitly, or discovered. An explicit '' is the opt-out: it disables auth even when the
	// default verifier is defined, which auto-detection alone would give no way to say.
	auto auth = input.named_parameters.find("auth_function");
	if (auth != input.named_parameters.end() && !auth->second.IsNull()) {
		result->auth_function = auth->second.GetValue<string>();
	} else if (ServeFunctionIsDefined(context, DEFAULT_AUTH_FN_NAME)) {
		result->auth_function = DEFAULT_AUTH_FN_NAME;
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> N6kServeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<N6kServeState>();
}

static void N6kServeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kServeBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	// Blocks the CALLING thread for the connection lifetime — the CALL is the server. Not a `SET
	// threads` slot: a synchronous CALL runs its own root task inline, so the cost is one host thread.
	auto &db = DatabaseInstance::GetDatabase(context);
	n6k::UdsConnection conn;
	if (bind_data.fd >= 0) {
		conn.AdoptBlockingDupOfFd(bind_data.fd);
	} else {
		conn.Connect(bind_data.socket_path);
	}
	n6k::ServeReactor reactor(conn, db, context, bind_data.catalogs);
	if (bind_data.ping_interval_ms >= 0) {
		reactor.SetPingIntervalMs(static_cast<int>(bind_data.ping_interval_ms));
	}
	if (!bind_data.auth_function.empty()) {
		reactor.SetAuthFunction(bind_data.auth_function);
	}
	int64_t handled = reactor.Run();

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value::BIGINT(handled));
	output.SetValue(2, 0, Value(bind_data.socket_path));

	state.done = true;
	output.SetCardinality(1);
}

void RegisterN6kServeFunction(ExtensionLoader &loader) {
	// Varargs with no fixed parameters accepts every arity: n6k_serve_socket() serves all
	// attached catalogs, n6k_serve_socket('a', 'b', ...) serves exactly the named ones.
	TableFunction func(FN_NAME, {}, N6kServeScan, N6kServeBind, N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

// Tells every client currently attached to `catalog` that those schemas changed, so the next lookup
// refetches instead of serving a stale cache. The server never decides on its own that a catalog has
// changed — the host says so.
static unique_ptr<FunctionData> N6kPushInvalidateBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names = {"notified"};
	return_types = {LogicalType::BIGINT};

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires a catalog: CALL %s('mydata', 'main')", PUSH_FN_NAME, PUSH_FN_NAME);
	}
	auto result = make_uniq<N6kPushInvalidateBindData>();
	result->catalog = input.inputs[0].GetValue<string>();
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("%s: schema names must not be NULL", PUSH_FN_NAME);
		}
		result->schemas.push_back(input.inputs[i].GetValue<string>());
	}
	if (result->schemas.empty()) {
		throw BinderException("%s requires at least one schema name: CALL %s('mydata', 'main')", PUSH_FN_NAME,
		                      PUSH_FN_NAME);
	}
	return std::move(result);
}

static void N6kPushInvalidateScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kPushInvalidateBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	const int notified = n6k::BroadcastInvalidate(db, bind_data.catalog, bind_data.schemas);

	output.SetValue(0, 0, Value::BIGINT(notified));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kServeStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names = {"connections", "requests_handled", "credit_pauses", "cancels"};
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	return make_uniq<N6kServeBindData>();
}

static void N6kServeStatsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto stats = n6k::CollectServeStats(DatabaseInstance::GetDatabase(context));
	output.SetValue(0, 0, Value::BIGINT(stats.connections));
	output.SetValue(1, 0, Value::BIGINT(stats.requests_handled));
	output.SetValue(2, 0, Value::BIGINT(stats.credit_pauses));
	output.SetValue(3, 0, Value::BIGINT(stats.cancels));
	state.done = true;
	output.SetCardinality(1);
}

void RegisterN6kServeStatsFunction(ExtensionLoader &loader) {
	TableFunction func(STATS_FN_NAME, {}, N6kServeStatsScan, N6kServeStatsBind, N6kServeInitGlobal);
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

void RegisterN6kServePushInvalidateFunction(ExtensionLoader &loader) {
	TableFunction func(PUSH_FN_NAME, {LogicalType::VARCHAR}, N6kPushInvalidateScan, N6kPushInvalidateBind,
	                   N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

void RegisterN6kServeFdFunction(ExtensionLoader &loader) {
	TableFunction func(FD_FN_NAME, {LogicalType::BIGINT}, N6kServeScan, N6kServeFdBind, N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	func.named_parameters["ping_interval_ms"] = LogicalType::BIGINT;
	func.named_parameters["auth_function"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
