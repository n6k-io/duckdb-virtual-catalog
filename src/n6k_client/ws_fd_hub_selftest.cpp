#include "ws_fd_hub_selftest.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include <chrono>
#include <string>
#include <vector>

#if !defined(WASM_LOADABLE_EXTENSIONS) && !defined(_WIN32)
#define N6K_HAS_FD_TRANSPORT 1
#include "n6k_msgpack.hpp"
#include "ws_transport_fd.hpp"

#include <mutex>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace {

struct WsFdHubResult {
	std::string mode = "native";
	bool ok = false;
	int32_t got_ns1 = 0;
	int32_t got_ns2 = 0;
	// Each session saw exactly its own ns (plus the untagged broadcast).
	bool ns1_only_own = false;
	bool ns2_only_own = false;
	// An untagged frame reaches every bound session (PONG and a pre-mux HELLO_ACK carry no ns).
	bool untagged_broadcast = false;
	// A frame naming a session nobody claims is dropped, never handed to a sibling.
	bool unknown_ns_dropped = false;
	// Both transports resolved to ONE hub, i.e. one reader owns the fd.
	bool single_hub = false;
	int64_t elapsed_ms = 0;
};

#ifdef N6K_HAS_FD_TRANSPORT

std::string MakeFrame(int64_t ns) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(ns != 0 ? 2 : 1);
	n6k::PackString(pk, "t");
	pk.pack(3);
	if (ns != 0) {
		n6k::PackString(pk, "ns");
		pk.pack(ns);
	}
	return std::string(sb.data(), sb.size());
}

bool WriteFramed(int fd, const std::string &frame) {
	std::string buf;
	uint32_t len = static_cast<uint32_t>(frame.size());
	buf.push_back(static_cast<char>((len >> 24) & 0xff));
	buf.push_back(static_cast<char>((len >> 16) & 0xff));
	buf.push_back(static_cast<char>((len >> 8) & 0xff));
	buf.push_back(static_cast<char>(len & 0xff));
	buf.append(frame);
	size_t sent = 0;
	while (sent < buf.size()) {
		ssize_t w = ::write(fd, buf.data() + sent, buf.size() - sent);
		if (w <= 0) {
			return false;
		}
		sent += static_cast<size_t>(w);
	}
	return true;
}

struct Sink {
	std::mutex mu;
	std::vector<int64_t> ns_seen;

	void Record(const std::string &frame) {
		int64_t ns = 0;
		try {
			size_t off = 0;
			auto oh = msgpack::unpack(frame.data(), frame.size(), off);
			if (oh.get().type == msgpack::type::MAP) {
				ns = n6k::GetInt(oh.get(), "ns", 0);
			}
		} catch (...) {
			ns = -1;
		}
		std::lock_guard<std::mutex> lk(mu);
		ns_seen.push_back(ns);
	}

	std::vector<int64_t> Snapshot() {
		std::lock_guard<std::mutex> lk(mu);
		return ns_seen;
	}
};

int CountOf(const std::vector<int64_t> &values, int64_t want) {
	int n = 0;
	for (auto v : values) {
		if (v == want) {
			n++;
		}
	}
	return n;
}

WsFdHubResult RunWsFdHubSelftest() {
	WsFdHubResult r;
	const auto t0 = std::chrono::steady_clock::now();
	const auto stamp = [&]() {
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	};

	int sv[2] = {-1, -1};
	if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
		r.mode = "socketpair_failed";
		stamp();
		return r;
	}
	const int client_fd = sv[0];
	const int peer_fd = sv[1];

	Sink sink1;
	Sink sink2;
	{
		// Both sessions ride the SAME fd, exactly as two ATTACHes with the same wsFd would.
		n6k::FdTransport t1(client_fd, /*ns=*/1);
		n6k::FdTransport t2(client_fd, /*ns=*/2);

		t1.Start([&sink1](const std::string &bytes, bool) { sink1.Record(bytes); }, nullptr, nullptr);
		t2.Start([&sink2](const std::string &bytes, bool) { sink2.Record(bytes); }, nullptr, nullptr);

		r.single_hub = n6k::FdHub::Acquire(client_fd)->SubscriberCount() == 2;

		WriteFramed(peer_fd, MakeFrame(1));
		WriteFramed(peer_fd, MakeFrame(2));
		WriteFramed(peer_fd, MakeFrame(0));  // untagged: connection-scoped, goes to both
		WriteFramed(peer_fd, MakeFrame(99)); // nobody's session: dropped

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (std::chrono::steady_clock::now() < deadline) {
			if (sink1.Snapshot().size() >= 2 && sink2.Snapshot().size() >= 2) {
				break;
			}
			::usleep(2000);
		}
		// Give a stray/misrouted frame a chance to land before asserting it did not.
		::usleep(50000);

		t1.Stop();
		t2.Stop();
	}

	const auto seen1 = sink1.Snapshot();
	const auto seen2 = sink2.Snapshot();
	r.got_ns1 = static_cast<int32_t>(seen1.size());
	r.got_ns2 = static_cast<int32_t>(seen2.size());
	r.ns1_only_own = CountOf(seen1, 1) == 1 && CountOf(seen1, 2) == 0;
	r.ns2_only_own = CountOf(seen2, 2) == 1 && CountOf(seen2, 1) == 0;
	r.untagged_broadcast = CountOf(seen1, 0) == 1 && CountOf(seen2, 0) == 1;
	r.unknown_ns_dropped = CountOf(seen1, 99) == 0 && CountOf(seen2, 99) == 0;
	r.ok = r.single_hub && r.ns1_only_own && r.ns2_only_own && r.untagged_broadcast && r.unknown_ns_dropped &&
	       r.got_ns1 == 2 && r.got_ns2 == 2;

	::close(peer_fd);
	::close(client_fd);
	stamp();
	return r;
}

#else

WsFdHubResult RunWsFdHubSelftest() {
	WsFdHubResult r;
	r.mode = "no_fd_transport";
	r.ok = true;
	return r;
}

#endif

struct N6kWsFdHubBind : public TableFunctionData {};

struct N6kWsFdHubState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode",
	         "ok",
	         "got_ns1",
	         "got_ns2",
	         "ns1_only_own",
	         "ns2_only_own",
	         "untagged_broadcast",
	         "unknown_ns_dropped",
	         "single_hub",
	         "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::INTEGER};
	return make_uniq<N6kWsFdHubBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kWsFdHubState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kWsFdHubState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const WsFdHubResult r = RunWsFdHubSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::INTEGER(r.got_ns1));
	output.SetValue(3, 0, Value::INTEGER(r.got_ns2));
	output.SetValue(4, 0, Value::BOOLEAN(r.ns1_only_own));
	output.SetValue(5, 0, Value::BOOLEAN(r.ns2_only_own));
	output.SetValue(6, 0, Value::BOOLEAN(r.untagged_broadcast));
	output.SetValue(7, 0, Value::BOOLEAN(r.unknown_ns_dropped));
	output.SetValue(8, 0, Value::BOOLEAN(r.single_hub));
	output.SetValue(9, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestFdHubRouting(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_fd_hub_routing", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
