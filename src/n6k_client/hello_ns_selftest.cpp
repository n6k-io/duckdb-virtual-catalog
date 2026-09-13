// `n6k_selftest_hello_ns_distinct()` — pins the FT_HELLO that a native attach actually puts on the wire.
//
// PackHello omits the `ns` key entirely when ns == 0 (ws_msgpack.hpp), and for a long time the native
// CatalogSession::Create never assigned one — so every native attach sent a HELLO with no session name.
// Against a single-catalog serve that is invisible, because it never reads `ns`; against a
// multiplexed serve it is fatal, and the frame is indistinguishable from a pre-mux client build
// (src/n6k_server/serve_reactor.cpp). This asserts the key is present and nonzero, and that two successive
// attaches get distinct values — a shared wsFd puts both on one socket, where a collision would make
// the router treat the second HELLO as a duplicate of the first.
//
// Drives the REAL CatalogSession::Create (not a hand-built WsClientOptions, which would just restate
// the code under test) against an in-process ix::WebSocketServer that answers HELLO_ACK so Connect()
// returns immediately instead of burning its 5s timeout.
//
// Two build regimes:
//   - native (!WASM_LOADABLE_EXTENSIONS): the real dial path.
//   - wasm (any variant): no ix transport and no in-process socket server; report mode='wasm_no_ix'.

#include "hello_ns_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <chrono>
#include <cstdint>
#include <string>

#ifndef WASM_LOADABLE_EXTENSIONS
#include "n6k_catalog_session.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"

#include <atomic>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#endif

namespace duckdb {

namespace {

struct HelloNsResult {
	std::string mode;
	bool ok = false;
	bool listening = false;
	int32_t hellos_seen = 0;
	bool ns_present = false; // every captured HELLO carried an `ns` key
	int64_t ns_first = 0;
	int64_t ns_second = 0;
	bool distinct = false;
	int64_t elapsed_ms = 0;
};

#ifndef WASM_LOADABLE_EXTENSIONS

constexpr int kAttachCount = 2;

// Minimal HELLO_ACK: WsClient::Connect only gates on protocol_version matching, and no
// session_pending means the ready-gate opens immediately.
std::string PackHelloAck() {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::HELLO_ACK));
	n6k::PackString(pk, "protocol_version");
	pk.pack(n6k::N6K_PROTOCOL_VERSION);
	n6k::PackString(pk, "max_concurrent_reqs");
	pk.pack(8);
	n6k::PackString(pk, "default_batch_credits");
	pk.pack(8);
	return std::string(sbuf.data(), sbuf.size());
}

int CandidateBasePort() {
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 41000 + static_cast<int>(tid % 2000);
}

HelloNsResult RunHelloNsSelftest() {
	HelloNsResult r;
	r.mode = "native";
	const auto t0 = std::chrono::steady_clock::now();
	auto stamp = [&]() {
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	};

	std::mutex mu;
	std::vector<int64_t> ns_values; // one per HELLO, 0 when the key was absent
	bool all_had_ns = true;

	std::unique_ptr<ix::WebSocketServer> server;
	const int base = CandidateBasePort();
	int port = 0;
	for (int attempt = 0; attempt < 16 && !r.listening; ++attempt) {
		const int candidate_port = base + attempt;
		std::unique_ptr<ix::WebSocketServer> candidate(new ix::WebSocketServer(candidate_port, "127.0.0.1"));
		candidate->disablePerMessageDeflate();
		candidate->setOnClientMessageCallback(
		    [&mu, &ns_values, &all_had_ns](const std::shared_ptr<ix::ConnectionState> &, ix::WebSocket &ws,
		                                   const ix::WebSocketMessagePtr &msg) {
			    if (msg->type != ix::WebSocketMessageType::Message || !msg->binary) {
				    return;
			    }
			    msgpack::object_handle oh;
			    size_t off = 0;
			    try {
				    oh = msgpack::unpack(msg->str.data(), msg->str.size(), off);
			    } catch (...) {
				    return;
			    }
			    const msgpack::object &h = oh.get();
			    if (h.type != msgpack::type::MAP) {
				    return;
			    }
			    if (static_cast<n6k::FrameType>(n6k::GetInt(h, "t", -1)) != n6k::FrameType::HELLO) {
				    return;
			    }
			    {
				    std::lock_guard<std::mutex> lk(mu);
				    if (!n6k::HasKey(h, "ns")) {
					    all_had_ns = false;
				    }
				    ns_values.push_back(n6k::GetInt(h, "ns", 0));
			    }
			    ws.sendBinary(PackHelloAck());
		    });
		if (!candidate->listen().first) {
			continue;
		}
		candidate->start();
		server = std::move(candidate);
		r.listening = true;
		port = candidate_port;
	}
	if (!r.listening) {
		stamp();
		return r;
	}

	const std::string base_url = "http://127.0.0.1:" + std::to_string(port);
	// Hold the sessions so neither is torn down (and its ns conceptually released) before the second
	// attach runs — two live attaches is exactly the case a shared socket has to keep apart.
	std::vector<std::shared_ptr<CatalogSession>> sessions;
	for (int i = 0; i < kAttachCount; ++i) {
		try {
			sessions.push_back(CatalogSession::Create(base_url, /*token=*/"", "nstest" + std::to_string(i)));
		} catch (...) {
			// Connect failure is reported through hellos_seen/ok below, not swallowed silently.
		}
	}

	{
		std::lock_guard<std::mutex> lk(mu);
		r.hellos_seen = static_cast<int32_t>(ns_values.size());
		r.ns_present = all_had_ns && !ns_values.empty();
		if (!ns_values.empty()) {
			r.ns_first = ns_values[0];
		}
		if (ns_values.size() > 1) {
			r.ns_second = ns_values[1];
		}
		r.distinct = ns_values.size() == kAttachCount && ns_values[0] != ns_values[1];
	}

	sessions.clear();
	server->stop();

	r.ok = r.listening && r.hellos_seen == kAttachCount && r.ns_present && r.ns_first != 0 && r.ns_second != 0 &&
	       r.distinct;
	stamp();
	return r;
}

#else

HelloNsResult RunHelloNsSelftest() {
	HelloNsResult r;
	r.mode = "wasm_no_ix";
	r.ok = true;
	return r;
}

#endif

struct N6kHelloNsBind : public TableFunctionData {};

struct N6kHelloNsState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode",     "ok",        "listening",   "hellos_seen", "ns_present",
	         "ns_first", "ns_second", "ns_distinct", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BOOLEAN, LogicalType::INTEGER};
	return make_uniq<N6kHelloNsBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kHelloNsState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kHelloNsState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const HelloNsResult r = RunHelloNsSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::BOOLEAN(r.listening));
	output.SetValue(3, 0, Value::INTEGER(r.hellos_seen));
	output.SetValue(4, 0, Value::BOOLEAN(r.ns_present));
	output.SetValue(5, 0, Value::BIGINT(r.ns_first));
	output.SetValue(6, 0, Value::BIGINT(r.ns_second));
	output.SetValue(7, 0, Value::BOOLEAN(r.distinct));
	output.SetValue(8, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestHelloNsDistinct(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_hello_ns_distinct", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
