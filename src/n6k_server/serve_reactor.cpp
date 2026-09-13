#include "serve_reactor.hpp"

#include "frame_codec.hpp"
#include "n6k_exception_types.hpp"
#include "n6k_sql_builder.hpp"
#include "n6k_protocol_generated.hpp"
#include "request_handlers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"

#include <chrono>
#include <cstdlib>
#include <memory>

namespace duckdb {
namespace n6k {

// Bounded so a stuck query cannot pin the serving CALL open forever.
static constexpr int SHUTDOWN_DRAIN_TIMEOUT_MS = 5000;

// Bounds the pre-HELLO wait so a silent peer cannot hold a reactor (and its thread) open.
static constexpr int64_t HANDSHAKE_TIMEOUT_MS = 5000;

// Idle connections get reaped by proxies, and a client-side timer is no substitute — browsers
// throttle those in background tabs. 0 or negative disables.
static constexpr const char *PING_INTERVAL_ENV = "N6K_PING_INTERVAL";
static constexpr double DEFAULT_PING_INTERVAL_S = 25.0;

static int ResolvePingIntervalMs() {
	const char *raw = std::getenv(PING_INTERVAL_ENV);
	double seconds = DEFAULT_PING_INTERVAL_S;
	if (raw != nullptr && raw[0] != '\0') {
		try {
			seconds = std::stod(std::string(raw));
		} catch (const std::exception &) {
			seconds = DEFAULT_PING_INTERVAL_S; // unparseable: keep the default rather than disable
		}
	}
	if (seconds <= 0) {
		return 0;
	}
	return static_cast<int>(seconds * 1000);
}

ServeReactor::ServeReactor(ServeTransport &conn, DatabaseInstance &db, ClientContext &context,
                           std::vector<std::string> catalogs)
    : conn_(conn), db_(db), context_(context), served_(std::move(catalogs)) {
	D_ASSERT(!served_.empty()); // every serve function's bind rejects an empty catalog set
	served_set_.insert(served_.begin(), served_.end());
	mux_ = served_.size() > 1;
	ping_interval_ms_ = ResolvePingIntervalMs();
}

static std::string JoinQuoted(const std::vector<std::string> &names) {
	std::string out;
	for (auto &name : names) {
		if (!out.empty()) {
			out += ", ";
		}
		out += "\"" + name + "\"";
	}
	return out;
}

ServeReactor::~ServeReactor() {
	StopPinger();
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		writer_stop_ = true;
	}
	out_cv_.notify_all();
	if (writer_.joinable()) {
		writer_.join();
	}
}

void ServeReactor::PingLoop() {
	uint32_t ping_id = 0;
	while (true) {
		{
			std::unique_lock<std::mutex> lk(ping_mu_);
			if (ping_cv_.wait_for(lk, std::chrono::milliseconds(ping_interval_ms_), [&] { return ping_stop_; })) {
				return;
			}
		}
		if (stopping_.load()) {
			return;
		}
		ping_id++;
		SendFrame(PackPing(ping_id));
	}
}

void ServeReactor::StopPinger() {
	{
		std::lock_guard<std::mutex> lk(ping_mu_);
		ping_stop_ = true;
	}
	ping_cv_.notify_all();
	if (pinger_.joinable()) {
		pinger_.join();
	}
}

void ServeReactor::SendFrame(std::string frame) {
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		out_q_.push_back(std::move(frame));
	}
	out_cv_.notify_one();
}

std::shared_ptr<RequestSlot> ServeReactor::FindSlot(int64_t ns, uint32_t req_id) {
	std::lock_guard<std::mutex> lk(map_mu_);
	auto it = inflight_.find(InflightKey {ns, req_id});
	return it == inflight_.end() ? nullptr : it->second;
}

const std::string *ServeReactor::LookupSessionCatalog(int64_t ns) const {
	auto it = sessions_.find(ns);
	return it == sessions_.end() ? nullptr : &it->second;
}

// A client HELLO opens one session: {ns, catalog}. `ns` is the routing key every reply for that
// session is stamped with; `catalog` picks which of the served catalogs it addresses. A duplicate ns
// is answered, not dropped: there is no client-visible log channel, and a silent drop hangs the
// client's connect waiter.
void ServeReactor::HandleHello(const msgpack::object &header) {
	const auto ns = GetInt(header, "ns", 0);
	const std::string catalog = GetStr(header, "catalog");

	if (catalog.empty()) {
		if (mux_) {
			SendFrame(PackHelloErr(ns, "InvalidInputException",
			                       "this connection serves " + std::to_string(served_.size()) + " catalogs (" +
			                           JoinQuoted(served_) + "); HELLO must name one in its \"catalog\" field"));
			return;
		}
		// Single-catalog serve: an omitted catalog means "the one you have".
	} else if (served_set_.find(catalog) == served_set_.end()) {
		SendFrame(PackHelloErr(ns, "InvalidInputException",
		                       "catalog \"" + catalog + "\" is not served by this connection (serving " +
		                           JoinQuoted(served_) + ")"));
		return;
	}

	// Authorize before the session exists, so a rejected HELLO leaves nothing addressable behind.
	const std::string rejection =
	    AuthorizationRejectionReason(GetStr(header, "token"), catalog.empty() ? served_[0] : catalog);
	if (!rejection.empty()) {
		SendFrame(PackHelloErr(ns, "InvalidInputException", rejection));
		// End the connection, unlike every other HELLO_ERR here: leaving it open would make one
		// connection an unlimited credential-guessing budget.
		stopping_.store(true);
		return;
	}

	if (LookupSessionCatalog(ns) != nullptr) {
		// Not a duplicate: a single-catalog serve already acked ns=0 at connect, and a HELLO there is
		// the normal opening move for a client carrying its token in the frame (browsers cannot set an
		// upgrade header). A genuine duplicate on a multiplexed session is still refused.
		if (!mux_ && ns == 0) {
			return;
		}
		SendFrame(PackHelloErr(ns, "InvalidInputException",
		                       "duplicate HELLO for ns=" + std::to_string(ns) + "; that session is already open"));
		return;
	}

	sessions_[ns] = catalog.empty() ? served_[0] : catalog;
	TrackSession(ns, sessions_[ns]);
	SendFrame(PackHelloAck(ns));
}

std::string ServeReactor::AuthorizationRejectionReason(const std::string &token, const std::string &catalog) {
	if (auth_function_.empty()) {
		return std::string();
	}
	try {
		Connection conn(db_);
		// Prepared, never interpolated: the token is whatever the peer sent, and this is the one
		// place a caller-supplied string reaches SQL without passing through the request builders.
		auto stmt = conn.Prepare("SELECT " + QuoteIdent(auth_function_) + "(?, ?)");
		if (stmt->HasError()) {
			return "auth function \"" + auth_function_ + "\" is unusable: " + stmt->GetError();
		}
		// A bound vector<Value>, never the variadic Execute: that overload routes std::string through
		// Value::CreateValue<string>, which builds a BLOB, so a verifier declared (VARCHAR, VARCHAR)
		// never binds and every HELLO is refused. Only the positive auth test catches this.
		vector<Value> params;
		params.emplace_back(token);
		params.emplace_back(catalog);
		auto result = stmt->Execute(params);
		if (result->HasError()) {
			// A raising verifier is how the host says *why*, so its message is the rejection.
			return result->GetError();
		}
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return "auth function \"" + auth_function_ + "\" returned no rows";
		}
		const auto verdict = chunk->GetValue(0, 0);
		if (verdict.IsNull() || !verdict.GetValue<bool>()) {
			return "unauthorized";
		}
		return std::string();
	} catch (const std::exception &e) {
		return std::string(e.what());
	}
}

void ServeReactor::TrackSession(int64_t ns, const std::string &catalog) {
	std::lock_guard<std::mutex> lk(push_mu_);
	push_sessions_[ns] = catalog;
}

void ServeReactor::UntrackSession(int64_t ns) {
	std::lock_guard<std::mutex> lk(push_mu_);
	push_sessions_.erase(ns);
}

int ServeReactor::NotifyInvalidate(const std::string &catalog, const std::vector<std::string> &schemas) {
	if (schemas.empty()) {
		return 0;
	}
	std::vector<int64_t> targets;
	{
		std::lock_guard<std::mutex> lk(push_mu_);
		for (auto &entry : push_sessions_) {
			if (entry.second == catalog) {
				targets.push_back(entry.first);
			}
		}
	}
	// Enqueued, not written: the writer thread serialises, so a push never races a response mid-frame
	// and never blocks the caller on a slow socket.
	for (auto ns : targets) {
		SendFrame(PackPushInvalidate(ns, schemas));
	}
	return static_cast<int>(targets.size());
}

void ServeReactor::CloseSessionAndCancelItsRequests(int64_t ns) {
	if (sessions_.erase(ns) == 0) {
		return;
	}
	UntrackSession(ns);
	std::lock_guard<std::mutex> lk(map_mu_);
	for (auto &entry : inflight_) {
		if (entry.first.ns != ns) {
			continue;
		}
		{
			std::lock_guard<std::mutex> slk(entry.second->mu);
			entry.second->cancel.store(true);
		}
		entry.second->cv.notify_all();
	}
}

// True once a connection that can only get a session from a HELLO — several catalogs to choose
// between, or authorization to pass — has idled past the deadline without opening one. A single
// unauthenticated catalog has its session from connect, so there is nothing to wait for. Latches off
// after the first session: the keepalive PING, not this, is what detects a dead peer.
bool ServeReactor::HandshakeExpired(std::chrono::steady_clock::time_point started) const {
	const bool needs_hello = mux_ || !auth_function_.empty();
	if (!needs_hello || !sessions_.empty()) {
		return false;
	}
	const auto elapsed = std::chrono::steady_clock::now() - started;
	return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() > HANDSHAKE_TIMEOUT_MS;
}

void ServeReactor::InterruptWorkers() {
	std::lock_guard<std::mutex> lk(map_mu_);
	for (auto &entry : inflight_) {
		shared_ptr<ClientContext> ctx;
		{
			std::lock_guard<std::mutex> slk(entry.second->mu);
			ctx = entry.second->context;
		}
		if (ctx) {
			ctx->Interrupt();
		}
	}
}

bool ServeReactor::AcquireCredit(RequestSlot &slot) {
	std::unique_lock<std::mutex> lk(slot.mu);
	// Counted before waiting: the metric is that the window ran dry, which is exactly the condition
	// the predicate below is about to block on.
	if (slot.credits <= 0 && !slot.cancel.load() && !stopping_.load()) {
		credit_pauses_.fetch_add(1);
	}
	slot.cv.wait(lk, [&] { return slot.credits > 0 || slot.cancel.load() || stopping_.load(); });
	if (slot.cancel.load() || stopping_.load()) {
		return false;
	}
	slot.credits--;
	return true;
}

void ServeReactor::AddCredits(RequestSlot &slot, int n) {
	{
		std::lock_guard<std::mutex> lk(slot.mu);
		slot.credits += n;
	}
	slot.cv.notify_all();
}

void ServeReactor::WriterLoop() {
	while (true) {
		std::string frame;
		{
			std::unique_lock<std::mutex> lk(out_mu_);
			out_cv_.wait(lk, [&] { return !out_q_.empty() || writer_stop_; });
			if (out_q_.empty()) {
				return;
			}
			frame = std::move(out_q_.front());
			out_q_.pop_front();
		}
		try {
			conn_.WriteFrame(frame);
		} catch (const std::exception &) {
			// Peer is gone; the reader will hit EOF and shut down.
			stopping_.store(true);
			return;
		}
	}
}

namespace {

// Live reactors keyed by the DatabaseInstance they serve. Global because "who is currently serving
// this database" is a process-scoped question — both serve functions can run at once, and
// n6k_serve_http alone holds one reactor per client.
std::mutex &RegistryMutex() {
	static std::mutex mu;
	return mu;
}

std::vector<std::pair<DatabaseInstance *, ServeReactor *>> &RegistryEntries() {
	static std::vector<std::pair<DatabaseInstance *, ServeReactor *>> entries;
	return entries;
}

} // namespace

ServeReactorRegistration::ServeReactorRegistration(DatabaseInstance &db, ServeReactor &reactor) : reactor_(&reactor) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	RegistryEntries().emplace_back(&db, &reactor);
}

void ServeReactorRegistration::Release() {
	if (!reactor_) {
		return;
	}
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &entries = RegistryEntries();
	for (auto it = entries.begin(); it != entries.end(); ++it) {
		if (it->second == reactor_) {
			entries.erase(it);
			break;
		}
	}
	reactor_ = nullptr;
}

ServeReactorRegistration::~ServeReactorRegistration() {
	Release();
}

int BroadcastInvalidate(DatabaseInstance &db, const std::string &catalog, const std::vector<std::string> &schemas) {
	// Lock held across NotifyInvalidate so a reactor cannot unregister and be destroyed between being
	// found and being used. Safe to hold: NotifyInvalidate only enqueues, so it cannot block on the
	// socket or re-enter here.
	std::lock_guard<std::mutex> lk(RegistryMutex());
	int notified = 0;
	for (auto &entry : RegistryEntries()) {
		if (entry.first == &db) {
			notified += entry.second->NotifyInvalidate(catalog, schemas);
		}
	}
	return notified;
}

ServeStats CollectServeStats(DatabaseInstance &db) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	ServeStats stats;
	for (auto &entry : RegistryEntries()) {
		if (entry.first != &db) {
			continue;
		}
		stats.connections++;
		stats.requests_handled += entry.second->RequestsHandled();
		stats.credit_pauses += entry.second->CreditPauses();
		stats.cancels += entry.second->Cancels();
	}
	return stats;
}

int64_t ServeReactor::Run() {
	ServeReactorRegistration registration(db_, *this);
	writer_ = std::thread(&ServeReactor::WriterLoop, this);
	if (ping_interval_ms_ > 0) {
		pinger_ = std::thread(&ServeReactor::PingLoop, this);
	}

	if (!mux_ && auth_function_.empty()) {
		// Single catalog: the default session exists from connect, so a client that never sends HELLO
		// still works and the server speaks first with an unprompted HELLO_ACK. Not when authorizing —
		// this session predates any HELLO, so it would let a client skip the check entirely.
		sessions_[0] = served_[0];
		TrackSession(0, served_[0]);
		SendFrame(PackHelloAck());
	}
	// Multiplexed: no default session and no unprompted ack — a client must HELLO to name the catalog
	// it wants. Acking early would mark it ready before its session exists and would swallow a later
	// HELLO_ERR, since the client only reports one if it has not already seen an ack.

	// Poll with a timeout so Ctrl-C (interrupt) ends the loop even on an idle peer.
	const auto started = std::chrono::steady_clock::now();
	std::string frame;
	try {
		while (!stopping_.load()) {
			if (!conn_.WaitReadable(250)) {
				if (context_.IsInterrupted()) {
					break;
				}
				if (HandshakeExpired(started)) {
					const std::string why = auth_function_.empty()
					                            ? "this connection serves " + std::to_string(served_.size()) +
					                                  " catalogs (" + JoinQuoted(served_) + ") and needs one named"
					                            : "this connection requires an authorized FT_HELLO";
					SendFrame(
					    PackHelloErr(0, "InvalidInputException",
					                 "no FT_HELLO within " + std::to_string(HANDSHAKE_TIMEOUT_MS) + "ms; " + why));
					break;
				}
				continue;
			}
			if (!conn_.ReadFrame(frame)) {
				break;
			}
			msgpack::object_handle oh;
			const char *raw_body;
			size_t raw_body_len;
			ParseFrame(frame, oh, raw_body, raw_body_len);
			const msgpack::object &h = oh.get();
			const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
			const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
			// Absent `ns` reads as 0 — the default session, which is how every pre-mux client speaks.
			const auto ns = GetInt(h, "ns", 0);
			switch (type) {
			case FrameType::REQ: {
				const auto op = static_cast<uint8_t>(GetInt(h, "op", 0xff));
				const std::string *catalog = LookupSessionCatalog(ns);
				if (catalog == nullptr) {
					// Never guess: answering a SCAN from an arbitrary catalog returns wrong data with
					// no error.
					const std::string why = ns == 0 ? "this connection serves " + std::to_string(served_.size()) +
					                                      " catalogs (" + JoinQuoted(served_) +
					                                      "); send FT_HELLO with an `ns` and a `catalog` first"
					                                : "unknown session ns=" + std::to_string(ns);
					SendError(*this, FrameTarget {ns, req_id}, "InvalidInputException", why);
					break;
				}
				// Reconstruct the JSON args body; INSERT/RPC_TABLE append raw Arrow after a newline.
				std::string body = HeaderPayloadJsonExcludingRouting(h);
				if (op == OP_INSERT || op == OP_RPC_TABLE) {
					body.push_back('\n');
					body.append(raw_body, raw_body_len);
				}
				AdmitAndSpawnRequestWorker(SessionRef {ns, *catalog}, req_id, op, std::move(body));
				break;
			}
			case FrameType::CREDIT: {
				const auto n = static_cast<int>(GetInt(h, "n", 0));
				auto slot = FindSlot(ns, req_id);
				if (slot && n > 0) {
					AddCredits(*slot, n);
				}
				break;
			}
			case FrameType::CANCEL: {
				// Reserved req id 0 = "close this session"; the client sends it on DETACH of a
				// multiplexed catalog. Guarded on ns so a pre-mux CANCEL{id:0} can't drop the default.
				if (req_id == 0 && ns != 0) {
					CloseSessionAndCancelItsRequests(ns);
					break;
				}
				auto slot = FindSlot(ns, req_id);
				if (slot) {
					cancels_.fetch_add(1);
					{
						std::lock_guard<std::mutex> lk(slot->mu);
						slot->cancel.store(true);
					}
					slot->cv.notify_all();
				}
				break;
			}
			case FrameType::PING:
				// Keepalive is connection-scoped: PONG is never ns-stamped, even if the PING was.
				SendFrame(PackPong(req_id));
				break;
			case FrameType::PONG:
				// No deadline is enforced; matched explicitly to distinguish it from an unknown type.
				break;
			case FrameType::HELLO:
				HandleHello(h);
				break;
			default:
				break;
			}
		}
	} catch (const std::exception &) {
		// A hard read error ends the session like an EOF would.
	}

	stopping_.store(true);
	// Leave the registry first, so a push arriving during teardown is not queued onto a writer that is
	// about to stop.
	registration.Release();
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		for (auto &entry : inflight_) {
			{ std::lock_guard<std::mutex> slk(entry.second->mu); }
			entry.second->cv.notify_all();
		}
	}
	{
		// The cancel flag only lands between chunks, so a worker inside a long query would sit there
		// until the query finished on its own. Interrupting its ClientContext unwinds it from inside
		// DuckDB instead.
		std::unique_lock<std::mutex> lk(workers_mu_);
		if (!workers_cv_.wait_for(lk, std::chrono::milliseconds(SHUTDOWN_DRAIN_TIMEOUT_MS),
		                          [&] { return active_workers_ == 0; })) {
			lk.unlock();
			InterruptWorkers();
			lk.lock();
			// Unbounded after the interrupt, deliberately: these workers are detached and hold a
			// reference to this reactor, so returning while one is live is a use-after-free. A
			// hang is recoverable; that is not.
			workers_cv_.wait(lk, [&] { return active_workers_ == 0; });
		}
	}
	StopPinger();
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		writer_stop_ = true;
	}
	out_cv_.notify_all();
	if (writer_.joinable()) {
		writer_.join();
	}
	return reqs_handled_.load();
}

void ServeReactor::AdmitAndSpawnRequestWorker(const SessionRef &sess, uint32_t req_id, uint8_t op, std::string body) {
	auto slot = std::make_shared<RequestSlot>();
	const InflightKey key {sess.ns, req_id};
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		if (inflight_.find(key) != inflight_.end()) {
			SendError(*this, FrameTarget {sess.ns, req_id}, "InvalidInputException",
			          "req_id " + std::to_string(req_id) + " in use");
			return;
		}
		// Enforce the window advertised in every HELLO_ACK. Each accepted REQ costs a detached
		// thread, so an unbounded queue is a client-driven thread bomb; refusing is what the
		// advertised limit means.
		if (inflight_.size() >= static_cast<size_t>(MAX_CONCURRENT_REQS)) {
			SendError(*this, FrameTarget {sess.ns, req_id}, "InvalidInputException",
			          "too many concurrent requests on this connection (limit " + std::to_string(MAX_CONCURRENT_REQS) +
			              ", as advertised in HELLO_ACK)");
			return;
		}
		inflight_.emplace(key, slot);
	}
	reqs_handled_.fetch_add(1);
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		active_workers_++;
	}
	std::thread(&ServeReactor::WorkerMain, this, sess, req_id, slot, op, std::move(body)).detach();
}

void ServeReactor::WorkerMain(const SessionRef &sess, uint32_t req_id, const std::shared_ptr<RequestSlot> &slot,
                              uint8_t op, const std::string &body) {
	try {
		Connection conn(db_);
		{
			std::lock_guard<std::mutex> lk(slot->mu);
			slot->context = conn.context;
		}
		HandleRequest(*this, conn, sess, op, body.data(), body.size(), req_id, *slot);
	} catch (const std::exception &e) {
		// The wire value is the DuckDB class name ("CatalogException"), not the display string
		// ExceptionTypeToString returns ("Catalog"). Sending the latter fails silently — the client
		// does not recognise it and downgrades to IOException, so every typed error becomes untyped.
		ErrorData err(e);
		SendError(*this, FrameTarget {sess.ns, req_id}, n6k::WireExceptionName(err.Type()), err.RawMessage(),
		          n6k::IsRetriableExceptionType(err.Type()));
	}

	{
		std::lock_guard<std::mutex> lk(map_mu_);
		inflight_.erase(InflightKey {sess.ns, req_id});
	}
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		active_workers_--;
	}
	workers_cv_.notify_all();
}

} // namespace n6k
} // namespace duckdb
