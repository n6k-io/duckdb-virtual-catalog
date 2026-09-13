// Transport-agnostic catalog session shared by native and wasm; only the Create factory differs by platform.

#include "n6k_body_builders.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_err_throw.hpp"
#include "n6k_fetch.hpp"
#include "n6k_io_thread.hpp" // shared I/O thread doorbell (self-guarded to coi/threads)
#include "n6k_protocol_generated.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_wasm_main_thread.hpp"
#include "n6k_ws_arrow_stream.hpp"
#include "ws_client.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

using n6k::FLAG_END_OF_STREAM;
using n6k::FLAG_IS_ARROW_IPC;
using n6k::FrameType;
using n6k::JsonGetBool;
using n6k::JsonGetStr;
using n6k::JsonGetUint;
using n6k::ThrowN6kError;
using n6k::UrlDecode;

namespace {

// The request is already on the wire when these wait, so an expired deadline is not "no result" -- the
// server may well have run the statement. Every collector below therefore has to tell a timeout apart
// from a clean end-of-stream and throw, or a stalled INSERT/EXEC reports "0 rows, no error".
constexpr std::chrono::seconds kResponseTimeout {30};
constexpr std::chrono::seconds kSchemaListTimeout {10};

void SendRequestAndCollectResponse(n6k::WsClient &client, const std::string &catalog, uint8_t op,
                                   const std::string &body, std::string &out_arrow, std::string &out_json_chunk,
                                   std::string &out_end_body) {
	auto state = client.AwaitReadySessionAndSendRequest(op, body);
	n6k::Frame f;
	bool saw_end = false;
	while (state->Next(f, kResponseTimeout)) {
		if (f.type == FrameType::ERR) {
			ThrowN6kError(catalog, op, f.payload);
		}
		if (f.type == FrameType::RESP_SCHEMA && (f.flags & FLAG_IS_ARROW_IPC)) {
			out_arrow.append(f.payload);
		} else if (f.type == FrameType::RESP_CHUNK) {
			if (f.flags & FLAG_IS_ARROW_IPC) {
				out_arrow.append(f.payload);
			} else if (out_json_chunk.empty()) {
				out_json_chunk = f.payload;
			}
		} else if (f.type == FrameType::RESP_END) {
			out_end_body = f.payload;
		}
		if ((f.flags & FLAG_END_OF_STREAM) != 0) {
			saw_end = true;
			break;
		}
	}
	if (!saw_end) {
		throw IOException("n6k[%s] %s: server did not finish responding within %llu seconds", catalog, n6k::OpName(op),
		                  static_cast<uint64_t>(kResponseTimeout.count()));
	}
}

void SendRequestAndReadArrow(n6k::WsClient &client, const std::string &catalog, uint8_t op, const std::string &body,
                             ArrowArrayStream *out) {
	std::string arrow, json_chunk, end_body;
	SendRequestAndCollectResponse(client, catalog, op, body, arrow, json_chunk, end_body);
	if (arrow.empty()) {
		throw IOException("n6k[%s] %s: server returned no Arrow payload", catalog, n6k::OpName(op));
	}
	auto *buf = static_cast<uint8_t *>(std::malloc(arrow.size()));
	if (!buf) {
		throw IOException("n6k[%s] %s: OOM allocating Arrow buffer (%lu bytes)", catalog, n6k::OpName(op),
		                  static_cast<uint64_t>(arrow.size()));
	}
	// NOLINTNEXTLINE(bugprone-not-null-terminated-result)
	std::memcpy(buf, arrow.data(), arrow.size());
	AdoptIpcBufferAsArrayStream(buf, static_cast<int64_t>(arrow.size()), out);
}

int64_t SendRequestAndReadRowcount(n6k::WsClient &client, const std::string &catalog, uint8_t op,
                                   const std::string &body) {
	std::string arrow, json_chunk, end_body;
	SendRequestAndCollectResponse(client, catalog, op, body, arrow, json_chunk, end_body);
	if (end_body.empty()) {
		return 0;
	}
	n6k::JsonDoc doc(end_body);
	if (!doc.Parsed()) {
		return 0;
	}
	return static_cast<int64_t>(JsonGetUint(doc.Root(), "rowcount"));
}

} // namespace

class WsCatalogSession : public CatalogSession {
public:
	WsCatalogSession(std::shared_ptr<n6k::WsClient> client, string base_url, string catalog_name)
	    : client_(std::move(client)), base_url_(std::move(base_url)), catalog_name_(std::move(catalog_name)) {
	}

	vector<string> ListSchemas() override {
		auto state = client_->AwaitReadySessionAndSendRequest(n6k::OP_CATALOG_LIST, std::string());
		vector<string> schemas;
		n6k::Frame f;
		bool saw_end = false;
		while (state->Next(f, kSchemaListTimeout)) {
			if (f.type == FrameType::ERR) {
				ThrowN6kError(catalog_name_, n6k::OP_CATALOG_LIST, f.payload);
			}
			if (f.type == FrameType::RESP_CHUNK) {
				n6k::JsonDoc doc(f.payload);
				if (doc.Parsed()) {
					schemas = n6k::JsonGetStrArray(doc.Root(), "schemas");
				}
			}
			if ((f.flags & FLAG_END_OF_STREAM) != 0) {
				saw_end = true;
				break;
			}
		}
		// An empty vector otherwise reads as "this catalog has no schemas", and the caller caches that.
		if (!saw_end) {
			throw IOException("n6k[%s] %s: server did not finish responding within %llu seconds", catalog_name_,
			                  n6k::OpName(n6k::OP_CATALOG_LIST), static_cast<uint64_t>(kSchemaListTimeout.count()));
		}
		return schemas;
	}

	vector<n6k::WsTableRow> ListTables() override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_TABLES_LIST, std::string("{}"), arrow,
		                              json_chunk, end_body);
		vector<n6k::WsTableRow> out;
		n6k::JsonDoc doc(json_chunk);
		if (!doc.Parsed()) {
			return out;
		}
		auto *arr = duckdb_yyjson::yyjson_obj_get(doc.Root(), "tables");
		if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
			return out;
		}
		size_t idx, n;
		duckdb_yyjson::yyjson_val *item;
		yyjson_arr_foreach(arr, idx, n, item) {
			if (!duckdb_yyjson::yyjson_is_obj(item)) {
				continue;
			}
			n6k::WsTableRow row;
			row.schema = JsonGetStr(item, "schema");
			row.name = JsonGetStr(item, "name");
			row.writable = JsonGetBool(item, "writable");
			row.editable = JsonGetBool(item, "editable");
			row.primary_keys = n6k::JsonGetStrArray(item, "primary_keys");
			out.emplace_back(std::move(row));
		}
		return out;
	}

	void FetchTableSchema(const string &schema, const string &table, ArrowArrayStream *out) override {
		SendRequestAndReadArrow(*client_, catalog_name_, n6k::OP_TABLE_SCHEMA, n6k_body::SchemaTable(schema, table),
		                        out);
	}

	void Scan(const string &schema, const string &table, const string &query, ArrowArrayStream *out,
	          StreamCancelCheck should_cancel = {}) override {
		auto body = n6k::UrlQueryToOpScanBody(schema, table, query);
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_SCAN, body);
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_SCAN, std::move(req), out,
		                                         std::move(should_cancel));
	}

	std::shared_ptr<n6k::RequestState> StartScanRequest(const string &schema, const string &table,
	                                                    const string &query) override {
		return StartOpRequest(n6k::OP_SCAN, n6k::UrlQueryToOpScanBody(schema, table, query));
	}

	std::shared_ptr<n6k::RequestState> StartOpRequest(uint8_t op, const string &body_json) override {
		return client_->AwaitReadySessionAndSendRequest(op, body_json);
	}

	bool HasCapability(const string &name) override {
		auto hello = client_->GetHello();
		for (auto &cap : hello.capabilities) {
			if (cap == name) {
				return true;
			}
		}
		return false;
	}

	int64_t Insert(const string &schema, const string &table, const uint8_t *data, size_t len) override {
		std::string body = n6k_body::SchemaTable(schema, table);
		body.push_back('\n');
		body.append(reinterpret_cast<const char *>(data), len);
		return SendRequestAndReadRowcount(*client_, catalog_name_, n6k::OP_INSERT, body);
	}

	int64_t Exec(const string &sql) override {
		return SendRequestAndReadRowcount(*client_, catalog_name_, n6k::OP_EXEC, n6k_body::Sql(sql));
	}

	void Query(const string &sql, ArrowArrayStream *out, StreamCancelCheck should_cancel = {}) override {
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_QUERY, n6k_body::Sql(sql));
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_QUERY, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void RpcScalar(const string &function, const string &args_json, ArrowArrayStream *out,
	               StreamCancelCheck should_cancel = {}) override {
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_RPC_SCALAR, n6k_body::Rpc(function, args_json));
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_RPC_SCALAR, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void RpcTable(const string &function, const string &args_json, const uint8_t *data, size_t len,
	              ArrowArrayStream *out, StreamCancelCheck should_cancel = {}) override {
		std::string body = n6k_body::Rpc(function, args_json);
		body.push_back('\n');
		body.append(reinterpret_cast<const char *>(data), len);
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_RPC_TABLE, body);
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_RPC_TABLE, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void CreateTable(const string &schema, const string &name, const string &columns_json) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_CREATE_TABLE,
		                              n6k_body::CreateTable(schema, name, columns_json), arrow, json_chunk, end_body);
	}

	void AlterTable(const string &schema, const string &table, const string &kind,
	                const string &details_json) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_ALTER_TABLE,
		                              n6k_body::AlterTable(schema, table, kind, details_json), arrow, json_chunk,
		                              end_body);
	}

	void SetPushHandler(PushHandler handler) override {
		client_->SetPushHandler(std::move(handler));
	}

	// Called unconditionally, but a no-op on native: NativeReactor does not override
	// Reactor::PollAndDispatchInbound, whose base implementation is empty. On wasm (no background
	// thread) the override drains the channel so idle-catalog PUSH fires.
	void PollPushEvents() override {
		if (client_) {
			client_->DrainInboundUnlessChannelReset();
		}
	}

	// Release the WebSocket on DETACH, else transport threads keep the WsClient alive forever. Idempotent.
	void Detach() override {
		if (detached_) {
			return;
		}
		detached_ = true;
		if (client_) {
			client_->Close();
		}
	}

private:
	std::shared_ptr<n6k::WsClient> client_;
	string base_url_;
	string catalog_name_;
	bool detached_ = false;
};

#ifdef WASM_LOADABLE_EXTENSIONS

// Bytes for one vsock channel: two 1 MiB SPSC byte-rings, each prefixed by 16 B of
// control words (HEAD/TAIL/CLOSED/EPOCH). MUST equal channelBytes(DEFAULT_CHANNEL_LAYOUT)
// in packages/npm/src/virtual-socket/channel.ts = 2 * ((1<<20) + 16). Both sides build
// Int32Array/Uint8Array views over these exact bytes, so any mismatch corrupts the rings.
static constexpr size_t N6K_VSOCK_CHANNEL_BYTES = 2 * ((size_t(1) << 20) + 16);

// Wasm: the ws-worker owns the socket; Create allocates the vsock channel + worker via JS before Connect sends HELLO.
std::shared_ptr<CatalogSession> CatalogSession::Create(const string &base_url, const string &token,
                                                       const string &catalog_name, const string &server_catalog,
                                                       const string &ws_id, int ws_fd,
                                                       std::chrono::milliseconds ready_timeout) {
	(void)ws_fd;
	const string hello_catalog = server_catalog.empty() ? catalog_name : server_catalog;
	// Per-attach session id lets ONE socket carry many catalogs; 0 (dial path) omits `ns` from the wire.
	static std::atomic<uint64_t> g_next_ns {1};
	uint64_t ns = 0;

	// Reserve the channel's ring region in duckdb's shared wasm heap (threaded/coi build
	// only) so the pump ws-worker — and later compute pthreads — address the same bytes
	// with no copy. Zero it so the ring control words start empty+open. Ownership then
	// passes to JS: it frees the region (via the wasm `free` export) on detach, once the
	// pump ws-worker has released the ring — C++ must NOT free it (that races the pump's
	// async teardown and would double-free). Without WITH_WASM_THREADS we pass ring_ptr=0
	// and JS falls back to a standalone SharedArrayBuffer (the pre-rebuild bootstrap path).
	void *ring = nullptr;
#ifdef WITH_WASM_THREADS
	ring = std::malloc(N6K_VSOCK_CHANNEL_BYTES);
	if (!ring) {
		throw IOException("n6k: failed to allocate %llu-byte vsock ring region",
		                  static_cast<unsigned long long>(N6K_VSOCK_CHANNEL_BYTES));
	}
	std::memset(ring, 0, N6K_VSOCK_CHANNEL_BYTES);
#endif
	const uintptr_t ring_ptr = reinterpret_cast<uintptr_t>(ring);

	// Shared doorbell address forwarded to the pump so it wakes the one I/O thread after publishing an
	// inbound frame. 0 unless the coi/threads build is active (then the I/O thread reads the rings).
	uintptr_t doorbell_ptr = 0;
#ifdef WITH_WASM_THREADS
	doorbell_ptr = reinterpret_cast<uintptr_t>(n6k::N6kIoThread::Instance().DoorbellAddress());
#endif

	std::string setup_js;
	if (!ws_id.empty()) {
		ns = g_next_ns.fetch_add(1);
		setup_js = "n6k.vsockAttachWs('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "', '" +
		           n6k::EscapeSingleQuotedJsLiteral(ws_id) + "', '" + n6k::EscapeSingleQuotedJsLiteral(token) + "', " +
		           std::to_string(ns) + ", " + std::to_string(ring_ptr) + ", " + std::to_string(doorbell_ptr) + ")";
	} else {
		// Pass the raw ws spec; JS resolves it (incl. a relative base_url the C++ side can't).
		string ws_spec = base_url + "/ws?catalog=" + hello_catalog;
		setup_js = "n6k.vsockAttach('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "', '" +
		           n6k::EscapeSingleQuotedJsLiteral(ws_spec) + "', '" + n6k::EscapeSingleQuotedJsLiteral(token) +
		           "', " + std::to_string(ring_ptr) + ", " + std::to_string(doorbell_ptr) + ")";
	}
	// Run the vsockAttach glue on the main runtime thread: under threads>1 duckdb can execute this
	// ATTACH on a compute pthread, whose JS scope has no globalThis.n6k (see n6k_wasm_main_thread.hpp).
	std::string setup_res = n6k::RunScriptStringOnMain(setup_js);
	if (setup_res.empty()) {
		setup_res = "ERROR:null from vsock attach";
	}
	if (setup_res.rfind("OK", 0) != 0) {
		std::free(ring);
		throw IOException("n6k: vsock attach failed: %s", setup_res);
	}

	// JS reports which backing it actually chose. "OK-heap" = it built ring views over our
	// malloc'd region, so the reactor drives it directly and JS owns the free on detach — keep
	// the malloc and hand the pointer to the reactor. "OK-sab" = JS ignored our pointer and used
	// a standalone SharedArrayBuffer (no capturedWasmMemory / non-threaded), so free our unused
	// region now and fall back to the JS-glue reactor (ring_ptr=0). No double-free: JS's
	// free-on-detach only fires for the heap path. In the non-threaded build `ring` is already
	// null and setup_res is "OK-sab", so std::free(nullptr) is a no-op and ring_ptr stays 0.
	uintptr_t reactor_ring_ptr = 0;
	if (setup_res.rfind("OK-heap", 0) == 0) {
		reactor_ring_ptr = ring_ptr;
	} else {
		std::free(ring);
	}

	n6k::WsClientOptions opts;
	opts.bearer_token = token;
	opts.connect_timeout = std::chrono::seconds(5);
	opts.ready_timeout = ready_timeout;
	opts.catalog = hello_catalog;
	opts.channel_key = catalog_name;
	opts.ns = ns;
	opts.ring_ptr = reactor_ring_ptr;

	auto client = n6k::WsClient::Create(opts);
	try {
		client->Connect();
	} catch (...) {
		// Connect throws on timeout, HELLO_ERR and version mismatch, and no session is returned on any
		// of them -- so nothing will ever call Detach, and the channel JS built above (pump worker, and
		// on the heap path the 2 MiB ring region it now owns) would leak for the life of the page. Ask
		// JS to close it, which is the same release path Detach takes. NOT std::free(ring): the pump's
		// teardown is async and owns those views, so freeing from here races it.
		n6k::RunScriptStringOnMain("n6k.vsockClose('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "')");
		throw;
	}
	return std::make_shared<WsCatalogSession>(std::move(client), base_url, catalog_name);
}

#else // native

std::shared_ptr<CatalogSession> CatalogSession::Create(const string &base_url, const string &token,
                                                       const string &catalog_name, const string &server_catalog,
                                                       const string &ws_id, int ws_fd,
                                                       std::chrono::milliseconds ready_timeout) {
	if (!ws_id.empty()) {
		throw IOException("n6k: wsId/registerWebsocket is only supported in the WASM/browser build");
	}

	const string hello_catalog = server_catalog.empty() ? catalog_name : server_catalog;

	// Per-attach session id. A dial gets its own socket so there is nothing to multiplex, but a
	// multiplexed serve rejects a HELLO that names no session — and an inherited wsFd may well BE a
	// shared socket. Stamping one unconditionally costs a map entry and works against both serve
	// modes: a single-catalog serve never reads `ns`.
	// Starts at 1 because PackHello omits the key when ns == 0, which is what a mux serve refuses.
	static std::atomic<uint64_t> g_next_ns {1};

	n6k::WsClientOptions opts;
	opts.bearer_token = token;
	opts.connect_timeout = std::chrono::seconds(5);
	opts.ready_timeout = ready_timeout;
	opts.catalog = hello_catalog;
	opts.ns = g_next_ns.fetch_add(1);

	if (ws_fd >= 0) {
		opts.ws_fd = ws_fd;
	} else {
		string ws_query = "?catalog=" + hello_catalog;
		if (StringUtil::StartsWith(base_url, "https://")) {
			opts.url = "wss://" + base_url.substr(8) + "/ws" + ws_query;
		} else if (StringUtil::StartsWith(base_url, "http://")) {
			opts.url = "ws://" + base_url.substr(7) + "/ws" + ws_query;
		} else {
			throw IOException("n6k: base URL must be http(s):// or n6k(s)://, got %s", base_url);
		}
	}

	auto client = n6k::WsClient::Create(opts);
	client->Connect();

	return std::make_shared<WsCatalogSession>(std::move(client), base_url, catalog_name);
}

#endif // WASM_LOADABLE_EXTENSIONS

} // namespace duckdb
