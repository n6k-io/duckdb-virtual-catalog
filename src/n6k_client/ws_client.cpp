#include "ws_client.hpp"
#include "ws_msgpack.hpp"
#include "n6k_protocol_generated.hpp"

#include <stdexcept>

namespace duckdb {
namespace n6k {

RequestState::RequestState(uint32_t req_id, std::weak_ptr<WsClient> owner) : req_id_(req_id), owner_(std::move(owner)) {
}

void RequestState::Push(Frame &&f) {
	std::function<void()> cb;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (finished_) {
			return;
		}
		bool end = (f.flags & FLAG_END_OF_STREAM) != 0 || f.type == FrameType::ERR;
		if (f.type == FrameType::ERR) {
			error_ = f.payload;
		}
		q_.push_back(std::move(f));
		if (end) {
			finished_ = true;
		}
		cb = on_frame_;
	}
	cv_.notify_all();
	// Fire outside the lock: the callback may re-enter this RequestState (TryNext) or reschedule the
	// pipeline (interrupt_state.Callback()); holding mu_ across it would risk deadlock.
	if (cb) {
		cb();
	}
}

void RequestState::DeliverSyntheticError(const std::string &msg) {
	std::function<void()> cb;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (finished_) {
			return;
		}
		error_ = msg;
		Frame f;
		f.type = FrameType::ERR;
		f.flags = FLAG_END_OF_STREAM;
		f.req_id = req_id_;
		f.payload = msg;
		q_.push_back(std::move(f));
		finished_ = true;
		cb = on_frame_;
	}
	cv_.notify_all();
	// A transport failure is terminal — wake any blocked scan so it observes the ERR and finishes.
	if (cb) {
		cb();
	}
}

bool RequestState::TryNext(Frame &out) {
	std::lock_guard<std::mutex> lk(mu_);
	if (!q_.empty()) {
		out = std::move(q_.front());
		q_.pop_front();
		return true;
	}
	return false;
}

void RequestState::SetOnFrame(std::function<void()> cb) {
	std::lock_guard<std::mutex> lk(mu_);
	on_frame_ = std::move(cb);
}

bool RequestState::Next(Frame &out, std::chrono::milliseconds timeout) {
	auto deadline = std::chrono::steady_clock::now() + timeout;
	auto owner = owner_.lock();
	const bool self_deliver = owner && owner->TransportSelfDelivers();
	for (;;) {
		{
			std::unique_lock<std::mutex> lk(mu_);
			if (!q_.empty()) {
				out = std::move(q_.front());
				q_.pop_front();
				return true;
			}
			if (finished_) {
				return false;
			}
			if (!owner || std::chrono::steady_clock::now() >= deadline) {
				return false;
			}
			if (self_deliver) {
				// A delivery thread Pushes+notifies cv_; park on it directly (no broadcast herd).
				cv_.wait_until(lk, deadline, [this] { return !q_.empty() || finished_; });
				continue;
			}
		}
		// Glue path (no delivery thread): drive the reactor to pull frames. Predicate self-locks mu_;
		// no lock held across the wait (Reactor contract).
		owner->WaitProgress(
		    [this] {
			    std::lock_guard<std::mutex> lk(mu_);
			    return !q_.empty() || finished_;
		    },
		    deadline);
	}
}

bool RequestState::Exhausted() const {
	std::lock_guard<std::mutex> lk(mu_);
	return finished_ && q_.empty();
}

void RequestState::Credit(uint32_t n) {
	if (auto owner = owner_.lock()) {
		owner->Credit(req_id_, n);
	}
}

void RequestState::Cancel() {
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (cancelled_ || finished_) {
			return;
		}
		cancelled_ = true;
	}
	if (auto owner = owner_.lock()) {
		owner->SendCancel(req_id_);
	}
}

std::shared_ptr<WsClient> WsClient::Create(WsClientOptions opts) {
	return std::shared_ptr<WsClient>(new WsClient(std::move(opts)));
}

WsClient::WsClient(WsClientOptions opts) : opts_(std::move(opts)) {
}

WsClient::~WsClient() {
	ShutdownFromOwner("client shutdown");
}

void WsClient::RecordHelloAck(HelloAck ack, bool session_pending) {
	std::lock_guard<std::mutex> lk(state_mu_);
	state_.hello = std::move(ack);
	state_.hello_arrived = true;
	state_.connected = true;
	// A deferred build sends FT_READY later; anything else is usable now.
	if (!session_pending) {
		state_.session_ready = true;
	}
}

void WsClient::RecordReady(bool has_capabilities, std::vector<std::string> capabilities) {
	std::lock_guard<std::mutex> lk(state_mu_);
	// A deferred build had no handler yet at HELLO_ACK time, so its capabilities ride the READY. They
	// must land before the gate opens, or the first request through it plans against a capability set
	// the server has already superseded.
	if (has_capabilities) {
		state_.hello.capabilities = std::move(capabilities);
	}
	state_.session_ready = true;
}

void WsClient::RecordHandshakeError(const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	// Neither gate is overwritten once it has an answer: a HELLO_ERR arriving after a good HELLO_ACK
	// belongs to a request, not to the handshake, and a drop reason already recorded is the first
	// and most specific one.
	if (!state_.hello_arrived) {
		state_.hello_error = msg;
		state_.hello_arrived = true;
	}
	if (!state_.session_ready) {
		if (state_.ready_error.empty()) {
			state_.ready_error = msg;
		}
		state_.session_ready = true;
	}
}

void WsClient::ResetHandshakeForReconnect() {
	std::lock_guard<std::mutex> lk(state_mu_);
	state_ = ConnState();
}

bool WsClient::HelloArrived() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.hello_arrived;
}

bool WsClient::SessionGateOpen() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.session_ready;
}

bool WsClient::IsConnected() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.connected;
}

std::shared_ptr<Reactor> WsClient::CurrentReactor() const {
	std::lock_guard<std::mutex> lk(reactor_mu_);
	return reactor_;
}

bool WsClient::TransportSelfDelivers() const {
	auto r = CurrentReactor();
	return r && r->SelfDelivers();
}

void WsClient::SendFrame(const std::string &frame_bytes) {
	if (auto r = CurrentReactor()) {
		r->Send(frame_bytes);
	}
}

bool WsClient::WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) {
	auto r = CurrentReactor();
	if (!r) {
		return ready();
	}
	return r->WaitUntil(ready, deadline);
}

void WsClient::DrainInboundUnlessChannelReset() {
	if (stopping_.load()) {
		return;
	}
	auto r = CurrentReactor();
	if (!r) {
		return;
	}
	// A moved epoch means the host reset the transport channel: this reactor belongs to the OLD
	// generation, and draining would swallow a NEW-generation frame — notably the reconnect
	// HELLO_ACK, which the server emits once and never resends, hanging the next request to its
	// deadline. Skip the drain and let AwaitReadySessionAndSendRequest's lazy Reconnect read it.
	// Not gated on connected_, so every idle poll in the reset window skips. Native reactors report
	// epoch 0 == connected_epoch_, so this never trips off-wasm.
	if (r->Epoch() != connected_epoch_.load()) {
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.connected = false;
		return;
	}
	r->PollAndDispatchInbound();
}

void WsClient::OnMessage(const std::string &bytes, bool binary) {
	if (!binary) {
		RecordHandshakeError("unexpected text frame from server");
		return;
	}
	// Bytes after the msgpack header map are the raw Arrow body.
	//
	// An unparseable frame carries no req_id, so there is no request to fail individually and a
	// silent drop hangs it to its deadline. Version skew or corruption is a connection-level fault:
	// fail every in-flight request, clear connected_, and let the lazy Reconnect rebuild.
	msgpack::object_handle oh;
	size_t off = 0;
	try {
		oh = msgpack::unpack(bytes.data(), bytes.size(), off);
	} catch (const std::exception &e) {
		FailConnectionAndAllInFlightRequests(std::string("malformed frame from server (msgpack): ") + e.what());
		return;
	}
	const msgpack::object &h = oh.get();
	if (h.type != msgpack::type::MAP) {
		FailConnectionAndAllInFlightRequests("malformed frame from server: header is not a msgpack map");
		return;
	}
	const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
	const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
	const char *body_ptr = bytes.data() + off;
	const size_t body_len = bytes.size() - off;

	if (type == FrameType::HELLO_ACK) {
		HelloAck ack;
		ack.protocol_version = static_cast<int>(GetInt(h, "protocol_version"));
		ack.max_concurrent_reqs = static_cast<uint32_t>(GetInt(h, "max_concurrent_reqs"));
		ack.default_batch_credits = static_cast<uint32_t>(GetInt(h, "default_batch_credits"));
		ack.capabilities = GetStrArray(h, "capabilities");
		// Set when the server defers a slow build and will send FT_READY later.
		RecordHelloAck(std::move(ack), GetBool(h, "session_pending"));
		return;
	}

	if (type == FrameType::READY) {
		// Deferred build finished; releases any request blocked on the ready-gate.
		const bool has_capabilities = HasKey(h, "capabilities");
		RecordReady(has_capabilities, has_capabilities ? GetStrArray(h, "capabilities") : std::vector<std::string>());
		return;
	}

	if (type == FrameType::HELLO_ERR) {
		// Connection-scoped failure surfaced to whichever waiter is blocked (Connect() or the ready-gate).
		const std::string etype = GetStr(h, "exception_type");
		const std::string emsg = GetStr(h, "exception_message");
		RecordHandshakeError(etype.empty() ? emsg : etype + ": " + emsg);
		return;
	}

	if (type == FrameType::PING) {
		SendFrame(PackPong(static_cast<int>(FrameType::PONG), HasKey(h, "id"), req_id));
		return;
	}

	if (type == FrameType::PUSH) {
		PushHandler hdl;
		{
			std::lock_guard<std::mutex> lk(push_handler_mu_);
			hdl = push_handler_;
		}
		if (hdl) {
			hdl(static_cast<uint8_t>(GetInt(h, "op")), HeaderPayloadJsonExcludingRouting(h));
		}
		return;
	}

	// Rebuild the legacy Frame: Arrow bodies stay raw; everything else reconstructs to JSON from the header.
	const bool is_arrow = (type == FrameType::RESP_SCHEMA) || HasKey(h, "arrow");
	const bool terminal = (type == FrameType::RESP_END) || (type == FrameType::ERR);

	Frame f;
	f.type = type;
	f.req_id = req_id;
	f.flags = 0;
	if (is_arrow) {
		f.flags |= FLAG_IS_ARROW_IPC;
		f.payload.assign(body_ptr, body_len);
	} else {
		f.payload = HeaderPayloadJsonExcludingRouting(h);
	}
	if (terminal) {
		f.flags |= FLAG_END_OF_STREAM;
	}

	std::shared_ptr<RequestState> st;
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		auto it = reqs_.find(req_id);
		if (it == reqs_.end()) {
			return;
		}
		st = it->second;
		if (terminal) {
			reqs_.erase(it);
		}
	}
	st->Push(std::move(f));
}

void WsClient::OnClose(const std::string &reason) {
	// Runs on the transport callback thread: must not stop the socket or join the writer.
	FailConnectionAndAllInFlightRequests(reason.empty() ? "socket closed" : reason);
}

std::string WsClient::HandshakeRejectionReason(bool hello_arrived) const {
	if (!hello_arrived) {
		return "n6k ws connect timeout";
	}
	std::string err;
	int version;
	{
		std::lock_guard<std::mutex> lk(state_mu_);
		err = state_.hello_error;
		version = state_.hello.protocol_version;
	}
	if (!err.empty()) {
		return err;
	}
	if (version != N6K_PROTOCOL_VERSION) {
		return "server protocol_version=" + std::to_string(version) + ", expected " +
		       std::to_string(N6K_PROTOCOL_VERSION);
	}
	return "";
}

HelloAck WsClient::Connect() {
	// Capture a strong self in reactor callbacks so the client outlives in-flight callbacks; ShutdownFromOwner breaks
	// the cycle.
	auto self = shared_from_this();
	auto r = std::shared_ptr<Reactor>(CreateReactor(opts_));
	r->Start([self](const std::string &bytes, bool binary) { self->OnMessage(bytes, binary); },
	         [self](const std::string &reason) { self->OnClose(reason); });
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		reactor_ = r;
	}

	SendFrame(PackHello(static_cast<int>(FrameType::HELLO), opts_.bearer_token, opts_.catalog, opts_.ns));

	auto deadline = std::chrono::steady_clock::now() + opts_.connect_timeout;
	bool got = WaitProgress([this] { return HelloArrived(); }, deadline);
	auto rejection = HandshakeRejectionReason(got);
	if (!rejection.empty()) {
		ShutdownFromOwner(rejection);
		throw std::runtime_error(rejection);
	}
	// Pin the epoch we connected on, so a later host-driven reset shows as a moved epoch in
	// AwaitReadySessionAndSendRequest.
	connected_epoch_.store(r->Epoch());
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.hello;
}

void WsClient::Reconnect() {
	// Serialize the whole rebuild: under threads>1 several compute pthreads can observe a disconnected
	// state and all call Reconnect. The first rebuilds; the rest re-check under this lock and bail, so
	// we never race the reactor_ swap or spawn duplicate service threads.
	std::lock_guard<std::mutex> reconnect_lk(reconnect_mu_);
	if (IsConnected() || stopping_.load()) {
		return;
	}
	ResetHandshakeForReconnect();
	auto self = shared_from_this();
	if (auto old = CurrentReactor()) {
		old->RetireKeepingChannelOpen();
	}
	auto r = std::shared_ptr<Reactor>(CreateReactor(opts_));
	r->Start([self](const std::string &bytes, bool binary) { self->OnMessage(bytes, binary); },
	         [self](const std::string &reason) { self->OnClose(reason); });
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		reactor_ = r;
	}
	SendFrame(PackHello(static_cast<int>(FrameType::HELLO), opts_.bearer_token, opts_.catalog, opts_.ns));
	auto deadline = std::chrono::steady_clock::now() + opts_.connect_timeout;
	bool got = WaitProgress([this] { return HelloArrived(); }, deadline);
	// Same guard Connect applies: a reconnect lands mid-deploy often enough that skipping the version
	// check here would let a client speak a protocol this build cannot read. RecordHelloAck marks the
	// state connected on ANY HELLO_ACK, so a rejected handshake has to clear it explicitly or the next
	// request proceeds.
	auto rejection = HandshakeRejectionReason(got);
	if (!rejection.empty()) {
		{
			std::lock_guard<std::mutex> lk(state_mu_);
			state_.connected = false;
		}
		// A peer that answered but is unusable will not become usable by retrying, so tear down. A bare
		// timeout leaves the client alive and a later request reconnects again.
		if (got) {
			ShutdownFromOwner(rejection);
		}
		throw std::runtime_error(rejection);
	}
	connected_epoch_.store(r->Epoch());
}

std::shared_ptr<RequestState> WsClient::AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body) {
	// Lazily re-establish a dropped (not closed) connection: rebuild + re-HELLO so a server drop recovers with no
	// re-ATTACH. A moved epoch means an out-of-band channel swap (wasm replaceWebsocket): clear `connected` to force a
	// fresh HELLO.
	if (IsConnected() && !stopping_.load()) {
		auto r = CurrentReactor();
		if (r && r->Epoch() != connected_epoch_.load()) {
			std::lock_guard<std::mutex> lk(state_mu_);
			state_.connected = false;
		}
	}
	if (!IsConnected() && !stopping_.load()) {
		Reconnect();
	}
	if (!IsConnected() || stopping_.load()) {
		throw std::runtime_error("ws client not connected");
	}
	// Session-ready gate: block until FT_READY, a connection-scoped error, or ready_timeout (cheap once the gate
	// is open).
	{
		auto deadline = std::chrono::steady_clock::now() + opts_.ready_timeout;
		bool ok = WaitProgress([this] { return SessionGateOpen(); }, deadline);
		if (!ok) {
			throw std::runtime_error("n6k session not ready after " + std::to_string(opts_.ready_timeout.count()) +
			                         "ms (server still building the session); raise the 'ready_timeout' ATTACH "
			                         "option if builds are slow");
		}
		std::lock_guard<std::mutex> lk(state_mu_);
		if (!state_.ready_error.empty()) {
			throw std::runtime_error(state_.ready_error);
		}
	}
	uint32_t req_id = next_req_id_.fetch_add(1);
	std::weak_ptr<WsClient> weak_self = shared_from_this();
	auto st = std::make_shared<RequestState>(req_id, weak_self);
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		reqs_[req_id] = st;
	}
	// INSERT/RPC_TABLE append a raw Arrow body after a `json-header\n` prefix; split it out here.
	const char *args = body.data();
	size_t args_len = body.size();
	const char *arrow = nullptr;
	size_t arrow_len = 0;
	if (op == OP_INSERT || op == OP_RPC_TABLE) {
		size_t nl = body.find('\n');
		if (nl != std::string::npos) {
			args_len = nl;
			arrow = body.data() + nl + 1;
			arrow_len = body.size() - nl - 1;
		}
	}
	SendFrame(PackReqFrame(static_cast<int>(FrameType::REQ), req_id, op, args, args_len, arrow, arrow_len, opts_.ns));
	return st;
}

void WsClient::SetPushHandler(PushHandler h) {
	std::lock_guard<std::mutex> lk(push_handler_mu_);
	push_handler_ = std::move(h);
}

void WsClient::Credit(uint32_t req_id, uint32_t n) {
	if (stopping_.load()) {
		return;
	}
	SendFrame(PackCredit(static_cast<int>(FrameType::CREDIT), req_id, n, opts_.ns));
}

void WsClient::SendCancel(uint32_t req_id) {
	if (stopping_.load()) {
		return;
	}
	SendFrame(PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), req_id, opts_.ns));
}

void WsClient::FailConnectionAndAllInFlightRequests(const std::string &reason) {
	// Both gates open with the drop reason before any request is failed, so a thread woken by the
	// synthetic error below already sees why rather than waiting out ready_timeout. RecordHandshakeError
	// leaves a gate that already has an answer alone.
	{
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.connected = false;
	}
	RecordHandshakeError(reason);

	std::unordered_map<uint32_t, std::shared_ptr<RequestState>> reqs;
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		reqs.swap(reqs_);
	}
	// Outside every client lock: DeliverSyntheticError fires the request's on_frame callback, which
	// reschedules a parked pipeline and must not run with state_mu_ held.
	for (auto &kv : reqs) {
		kv.second->DeliverSyntheticError(reason);
	}
}

void WsClient::Close() {
	ShutdownFromOwner("client closed");
}

bool WsClient::IsReady() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.session_ready && state_.ready_error.empty();
}

HelloAck WsClient::GetHello() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.hello;
}

std::string WsClient::LastError() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.ready_error.empty() ? state_.hello_error : state_.ready_error;
}

void WsClient::ShutdownFromOwner(const std::string &reason) {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}

	FailConnectionAndAllInFlightRequests(reason);

	// Stop the reactor: joins/releases its transport and destroys the self-holding callbacks, breaking the cycle.
	std::shared_ptr<Reactor> r;
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		r = std::move(reactor_);
	}
	if (r) {
		r->Stop();
	}
}

} // namespace n6k
} // namespace duckdb
