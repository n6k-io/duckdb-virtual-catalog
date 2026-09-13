#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "serve_transport.hpp"

// duckdb::shared_ptr is its own type, not an alias for std::shared_ptr.
#include "duckdb/common/shared_ptr.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

class DatabaseInstance;
class ClientContext;

namespace n6k {

// One multiplexed session: the `ns` every response for it is stamped with, and the catalog its ops are
// qualified against. ns == 0 is the default session — what a client that sends no HELLO lands on.
struct SessionRef {
	int64_t ns = 0;
	std::string catalog;
};

// Per-request state; the worker waits on cv, the reader signals it.
struct RequestSlot {
	std::atomic<bool> cancel {false};
	std::mutex mu;
	std::condition_variable cv;
	int credits = DEFAULT_BATCH_CREDITS;
	// The worker's own DuckDB context, published once it has one so shutdown can interrupt a query
	// that is not going to notice `cancel` on its own. Guarded by `mu`.
	shared_ptr<ClientContext> context;
};

// Requests are keyed by (ns, req_id), not req_id alone: each session owns its own req-id space, so two
// sessions may both have req_id 1 in flight without colliding.
struct InflightKey {
	int64_t ns;
	uint32_t req_id;

	bool operator==(const InflightKey &other) const {
		return ns == other.ns && req_id == other.req_id;
	}
};

struct InflightKeyHash {
	size_t operator()(const InflightKey &key) const {
		return std::hash<int64_t> {}(key.ns) ^ (std::hash<uint32_t> {}(key.req_id) << 1);
	}
};

// Serve engine: reader routes frames, a writer serializes output, each REQ gets its own worker thread.
class ServeReactor {
public:
	ServeReactor(ServeTransport &conn, DatabaseInstance &db, ClientContext &context, std::vector<std::string> catalogs);
	~ServeReactor();

	ServeReactor(const ServeReactor &) = delete;
	ServeReactor &operator=(const ServeReactor &) = delete;

	// Run the reader loop until EOF; returns the number of REQ frames accepted.
	int64_t Run();

	// 0 or negative disables pinging. Must be called before Run(); otherwise the interval comes from
	// $N6K_PING_INTERVAL (seconds, default 25).
	void SetPingIntervalMs(int ms) {
		ping_interval_ms_ = ms;
	}

	// Require every session to be authorized by a scalar function of (token, catalog) -> BOOLEAN,
	// resolved by name in the served database. Must be called before Run(). Only n6k_serve_fd
	// exposes this -- n6k_serve_socket and n6k_serve_http have no way to set it, so those forms
	// are unauthenticated.
	//
	// A SQL function rather than built-in auth because the credential arrives inside FT_HELLO —
	// browsers cannot set a header on a WebSocket upgrade — so whoever fronts this connection cannot
	// check it without parsing frames. The host keeps its own auth (JWKS, key rotation, session
	// lookup) wherever it already lives while frame handling stays here.
	//
	// Setting this also suppresses the connect-time default session, which exists before any HELLO and
	// would otherwise let a client skip the handshake.
	void SetAuthFunction(std::string function_name) {
		auth_function_ = std::move(function_name);
	}

	void SendFrame(std::string frame);

	// Consume one credit; false if cancelled or shutting down (caller stops streaming).
	bool AcquireCredit(RequestSlot &slot);

	void AddCredits(RequestSlot &slot, int n);

	// Push OP_CATALOG_INVALIDATED to every session on this connection bound to `catalog`, each
	// stamped with its own ns. Returns how many were notified. Safe to call from any thread.
	int NotifyInvalidate(const std::string &catalog, const std::vector<std::string> &schemas);

	// Backpressure and cancellation are invisible from outside — a correctly paced stream and one
	// that never filled its window look identical on the wire — so the conformance suite asserts on
	// these to prove flow control actually engaged.
	int64_t CreditPauses() const {
		return credit_pauses_.load();
	}
	int64_t Cancels() const {
		return cancels_.load();
	}
	int64_t RequestsHandled() const {
		return reqs_handled_.load();
	}

private:
	void WriterLoop();
	// Emits an FT_PING every ping_interval_ms until shutdown. Keepalive only: no PONG is awaited and
	// no deadline is enforced, so a briefly stalled client is never dropped for a late reply.
	void PingLoop();
	// Signal the pinger to stop and join it. Idempotent, so Run() and ~ServeReactor may both call it.
	void StopPinger();
	// Reader thread only.
	void HandleHello(const msgpack::object &header);
	// Reader thread only.
	void CloseSessionAndCancelItsRequests(int64_t ns);
	// Reader thread only.
	const std::string *LookupSessionCatalog(int64_t ns) const;
	// Reader thread only.
	bool HandshakeExpired(std::chrono::steady_clock::time_point started) const;

	void AdmitAndSpawnRequestWorker(const SessionRef &sess, uint32_t req_id, uint8_t op, std::string body);
	// Spawned on its own thread. std::thread decay-copies these, so `sess` stays valid even when the
	// reader closes that session mid-request — the worker must never reach back into sessions_.
	void WorkerMain(const SessionRef &sess, uint32_t req_id, const std::shared_ptr<RequestSlot> &slot, uint8_t op,
	                const std::string &body);
	std::shared_ptr<RequestSlot> FindSlot(int64_t ns, uint32_t req_id);
	// Interrupt every in-flight worker's DuckDB context. Called only from the shutdown path. A worker
	// that has not yet published its context has nothing to interrupt and will see `stopping_` instead.
	void InterruptWorkers();
	// Reader thread only.
	void TrackSession(int64_t ns, const std::string &catalog);
	void UntrackSession(int64_t ns);
	// Run the configured auth function. Returns empty on success, else the rejection reason.
	// Reader thread only, and blocking: a session's first frame is the right place to pay for this.
	std::string AuthorizationRejectionReason(const std::string &token, const std::string &catalog);

	ServeTransport &conn_;
	DatabaseInstance &db_;
	ClientContext &context_;
	// Catalogs this connection serves, in the order the serve function named them; never empty.
	// served_[0] backs the default session that a client which sends no HELLO lands on.
	std::vector<std::string> served_;
	std::unordered_set<std::string> served_set_;
	// True once more than one catalog is served: HELLO becomes mandatory and the unprompted
	// connect-time HELLO_ACK is suppressed (a client must name the session it wants).
	bool mux_ = false;

	// Open sessions by ns; key 0 is the default (untagged) session. Written and read ONLY by the
	// reader thread — workers copy the SessionRef they need, so no lock is required here.
	std::unordered_map<int64_t, std::string> sessions_;

	// A locked mirror of `sessions_` for the one caller that is not the reader thread: an invalidation
	// pushed from another connection needs to know which ns to stamp. Separate so the request path
	// keeps lock-free access to `sessions_`; touched only on session open/close and on a push.
	mutable std::mutex push_mu_;
	std::unordered_map<int64_t, std::string> push_sessions_;

	std::mutex out_mu_;
	std::condition_variable out_cv_;
	std::deque<std::string> out_q_;
	bool writer_stop_ = false;
	std::thread writer_;

	// Keepalive. Its own mutex/cv rather than the writer's, so that a connection under load does not
	// wake the pinger on every enqueued frame.
	std::mutex ping_mu_;
	std::condition_variable ping_cv_;
	bool ping_stop_ = false;
	std::thread pinger_;
	int ping_interval_ms_ = 0;

	// Empty disables authorization entirely, which is the default and what a standalone serve (no
	// host process to ask) necessarily runs with.
	std::string auth_function_;

	std::mutex map_mu_;
	std::unordered_map<InflightKey, std::shared_ptr<RequestSlot>, InflightKeyHash> inflight_;

	// Detached workers; counter lets Run() wait for drain at shutdown.
	std::mutex workers_mu_;
	std::condition_variable workers_cv_;
	int active_workers_ = 0;

	std::atomic<int64_t> reqs_handled_ {0};
	std::atomic<int64_t> credit_pauses_ {0};
	std::atomic<int64_t> cancels_ {0};
	std::atomic<bool> stopping_ {false};
};

// Live reactors on one DatabaseInstance.
//
// An invalidation originates outside the connection it must reach: whatever mutated the data holds
// its own DuckDB connection and has no handle on the sockets currently serving that catalog. So
// reactors register themselves for the duration of Run(), and a push is addressed by
// (DatabaseInstance, catalog) rather than by connection.
//
// Non-owning and multi-valued (several reactors per DatabaseInstance), which is why this is not the
// shared SharedRegistry in common/include/n6k_registry.hpp. The lifetime rule it does share: no
// registration without a matching removal.
//
// A registered reactor is reachable from any thread, so a push may arrive at any moment it is in the
// registry — and must never reach one that is tearing down. Scoping the registration to this guard
// is what makes that unmissable: it holds for exactly the window Run() can serve, and Release()
// closes that window early, before the workers drain.
class ServeReactorRegistration {
public:
	ServeReactorRegistration(DatabaseInstance &db, ServeReactor &reactor);
	~ServeReactorRegistration();

	ServeReactorRegistration(const ServeReactorRegistration &) = delete;
	ServeReactorRegistration &operator=(const ServeReactorRegistration &) = delete;

	// Deregister now rather than at scope exit. Idempotent.
	void Release();

private:
	ServeReactor *reactor_;
};

// Push an invalidation to every reactor on `db` serving `catalog`. Returns the number of sessions
// notified across all of them, which is what makes a no-op distinguishable from a delivery.
int BroadcastInvalidate(DatabaseInstance &db, const std::string &catalog, const std::vector<std::string> &schemas);

// Aggregate counters over every reactor currently serving `db`.
struct ServeStats {
	int64_t connections = 0;
	int64_t requests_handled = 0;
	int64_t credit_pauses = 0;
	int64_t cancels = 0;
};
ServeStats CollectServeStats(DatabaseInstance &db);

} // namespace n6k
} // namespace duckdb
