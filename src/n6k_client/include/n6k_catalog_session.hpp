#pragma once

#include "duckdb.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct ArrowArrayStream;

namespace duckdb {
namespace n6k {

struct WsTableRow {
	string schema;
	string name;
	bool writable = false;
	bool editable = false;
	vector<string> primary_keys;
};

class RequestState;

} // namespace n6k

// Polled between waits to abort a blocked stream; empty = never cancel. Native only (wasm ignores it).
using StreamCancelCheck = std::function<bool()>;

class CatalogSession {
public:
	virtual ~CatalogSession() = default;

	virtual vector<string> ListSchemas() = 0;
	virtual vector<n6k::WsTableRow> ListTables() = 0;
	virtual void FetchTableSchema(const string &schema, const string &table, ArrowArrayStream *out) = 0;
	virtual void Scan(const string &schema, const string &table, const string &query, ArrowArrayStream *out,
	                  StreamCancelCheck should_cancel = {}) = 0;
	// Start a scan and hand back the raw RequestState so an async source operator can drive it
	// frame-by-frame (TryNext / SetOnFrame / Credit) instead of the blocking Arrow stream. The request
	// is already in flight on return. Default null = the session can't expose it (async scan falls back
	// to the blocking Scan()).
	virtual std::shared_ptr<n6k::RequestState> StartScanRequest(const string & /*schema*/, const string & /*table*/,
	                                                            const string & /*query*/) {
		return nullptr;
	}
	// Generic form of the above: start any op with a pre-serialized JSON body. Keeps ops added later
	// (OP_AGGREGATE) off this interface. Default null = the session can't expose a RequestState.
	virtual std::shared_ptr<n6k::RequestState> StartOpRequest(uint8_t /*op*/, const string & /*body_json*/) {
		return nullptr;
	}
	// Does the connected server advertise `name` (HELLO_ACK / READY `capabilities`)? Asked per
	// attached catalog, because one duckdb instance can hold several n6k servers of different
	// vintages, and because the aggregate-pushdown rule must decide while planning -- a run-time
	// NotSupported comes too late to fall back.
	virtual bool HasCapability(const string & /*name*/) {
		return false;
	}
	virtual int64_t Insert(const string &schema, const string &table, const uint8_t *data, size_t len) = 0;
	virtual int64_t Exec(const string &sql) = 0;
	virtual void Query(const string &sql, ArrowArrayStream *out, StreamCancelCheck should_cancel = {}) = 0;
	virtual void RpcScalar(const string &function, const string &args_json, ArrowArrayStream *out,
	                       StreamCancelCheck should_cancel = {}) = 0;
	virtual void RpcTable(const string &function, const string &args_json, const uint8_t *data, size_t len,
	                      ArrowArrayStream *out, StreamCancelCheck should_cancel = {}) = 0;
	virtual void CreateTable(const string &schema, const string &name, const string &columns_json) = 0;
	virtual void AlterTable(const string &schema, const string &table, const string &kind,
	                        const string &details_json) = 0;

	// Handler for server-initiated PUSH frames (opcode, JSON body). Default no-op.
	using PushHandler = std::function<void(uint8_t op, const std::string &body)>;
	// NOLINTNEXTLINE(performance-unnecessary-value-param): native override moves the handler.
	virtual void SetPushHandler(PushHandler /*handler*/) {
	}

	// Synchronously drain queued PUSH events to the handler. Default no-op; WsCatalogSession
	// overrides it for both native and wasm.
	virtual void PollPushEvents() {
	}

	// Idempotently tear down the connection on DETACH (not when the last shared_ptr drops). Default no-op.
	virtual void Detach() {
	}

	// catalog_name = ATTACH alias; server_catalog defaults to it; ws_id/ws_fd ride a host socket; ready_timeout bounds
	// first request.
	static std::shared_ptr<CatalogSession>
	Create(const string &base_url, const string &token, const string &catalog_name, const string &server_catalog = "",
	       const string &ws_id = "", int ws_fd = -1,
	       std::chrono::milliseconds ready_timeout = std::chrono::milliseconds(60000));
};

} // namespace duckdb
