#pragma once

#include "n6k_protocol_generated.hpp"
#include "ws_reactor.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {
namespace n6k {

struct Frame {
	FrameType type;
	uint8_t flags;
	uint32_t req_id;
	std::string payload;
};

struct HelloAck {
	int protocol_version = 0;
	uint32_t max_concurrent_reqs = 0;
	uint32_t default_batch_credits = 0;
	std::vector<std::string> capabilities;
};

struct WsClientOptions {
	std::string url;
	std::string bearer_token;
	// SERVER-side catalog name; sent in the ?catalog= dial query and the FT_HELLO frame.
	std::string catalog;
	// Wasm only: vsock channel key = LOCAL ATTACH alias (unique per duckdb instance).
	std::string channel_key;
	// Session tag so ONE socket carries many catalog sessions; 0 = untagged single session.
	uint64_t ns = 0;
	// Wasm coi/threads only: heap address of the vsock channel's ring region, so the reactor
	// reads/writes the rings directly with native atomics. 0 = fall back to the JS-glue reactor.
	uintptr_t ring_ptr = 0;
	std::chrono::milliseconds connect_timeout {5000};
	// How long the first request waits for FT_READY on a deferred session build.
	std::chrono::milliseconds ready_timeout {60000};
	// >= 0: ride this already-connected socket fd instead of dialing url. Native only.
	int ws_fd = -1;
};

class WsClient;

class RequestState {
public:
	RequestState(uint32_t req_id, std::weak_ptr<WsClient> owner);

	bool Next(Frame &out, std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

	// Non-blocking peek: pops one frame if ready, else returns false immediately (never waits).
	// Like Next, returns false both when no frame is buffered yet and when the stream is finished;
	// callers distinguish via Exhausted(). Used by the async/BLOCKED scan to check without parking.
	bool TryNext(Frame &out);

	// Register a "frame arrived" callback, fired (outside the request lock) each time a frame is
	// pushed onto this request. The async scan uses it to fire interrupt_state.Callback(). The
	// callback must be safe to run on the reactor's inbound thread. Dormant until set.
	void SetOnFrame(std::function<void()> cb);

	// Idempotent best-effort CANCEL; server responds with a terminal RESP_END{cancelled}.
	void Cancel();

	void Credit(uint32_t n);

	uint32_t ReqId() const {
		return req_id_;
	}
	bool Exhausted() const;
	const std::string &ErrorMessage() const {
		return error_;
	}

private:
	friend class WsClient;

	void Push(Frame &&f);
	void DeliverSyntheticError(const std::string &msg);

	uint32_t req_id_;
	std::weak_ptr<WsClient> owner_;

	mutable std::mutex mu_;
	// Per-request wake (paired with mu_): a delivery thread notifies it from Push/DeliverSyntheticError so a
	// blocked Next wakes for THIS request only — no thundering herd across N concurrent requests.
	std::condition_variable cv_;
	std::deque<Frame> q_;
	bool finished_ = false;
	bool cancelled_ = false;
	std::string error_;
	// Fired (a copy, outside mu_) after each Push. Empty unless the async scan set it.
	std::function<void()> on_frame_;
};

class WsClient : public std::enable_shared_from_this<WsClient> {
public:
	static std::shared_ptr<WsClient> Create(WsClientOptions opts);
	~WsClient();

	// Opens the socket, blocks until HELLO_ACK; throws on failure/timeout/version mismatch.
	HelloAck Connect();

	std::shared_ptr<RequestState> AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body);

	void Credit(uint32_t req_id, uint32_t n);

	void SendCancel(uint32_t req_id);

	// Server-initiated PUSH dispatch; invoked on the WS callback thread. Set before Connect().
	using PushHandler = std::function<void(uint8_t op, const std::string &body)>;
	void SetPushHandler(PushHandler h);

	bool IsConnected() const;

	// True once the session is usable (HELLO_ACK w/o session_pending or later FT_READY) and no conn error.
	bool IsReady() const;
	HelloAck GetHello() const;
	std::string LastError() const;

	// Proactive teardown (idempotent): fail in-flight reqs + stop reactor. Never from an inbound callback.
	void Close();

	// Block until ready() or deadline. ready() must self-lock and hold no client mutex (Reactor contract).
	bool WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline);

	// Non-blocking: process available inbound frames (e.g. idle-connection PUSH). Native: no-op.
	void DrainInboundUnlessChannelReset();

	// See Reactor::SelfDelivers — lets RequestState::Next park on its own cv_ vs drive the reactor.
	bool TransportSelfDelivers() const;

private:
	// The whole handshake state of one connection, under one lock.
	//
	// No field here is meaningful on its own: `connected` without `hello_arrived` is a half-open
	// handshake, `session_ready` with a non-empty `ready_error` is a refusal rather than a gate that
	// opened, and `hello.capabilities` can still be replaced by a later READY. Every one of them is
	// written by the reactor's inbound thread and read by requesting threads, so a reader that
	// sampled them under separate locks could observe a combination that never existed. One value,
	// one lock, and no ordering rule for a caller to remember.
	struct ConnState {
		//! A HELLO_ACK or a HELLO_ERR has been seen. What Connect() waits for; says nothing about
		//! whether the answer was usable — `hello_error` does.
		bool hello_arrived = false;
		//! The session gate: opened by a HELLO_ACK without session_pending, or by a later READY.
		bool session_ready = false;
		//! A HELLO_ACK landed and no drop has been observed since.
		bool connected = false;
		HelloAck hello;
		//! Why the completed handshake is unusable; empty if it is fine.
		std::string hello_error;
		//! Why the session gate opened without a usable session; empty if it opened normally.
		std::string ready_error;
	};

	explicit WsClient(WsClientOptions opts);
	WsClient(const WsClient &) = delete;
	WsClient &operator=(const WsClient &) = delete;

	// Inbound-thread state transitions. Each takes state_mu_ for its whole update, so a reader never
	// sees half of one.
	void RecordHelloAck(HelloAck ack, bool session_pending);
	void RecordReady(bool has_capabilities, std::vector<std::string> capabilities);
	void RecordHandshakeError(const std::string &msg);
	//! Reset the handshake so a rebuilt reactor can run a fresh HELLO.
	void ResetHandshakeForReconnect();

	// Predicates for WaitProgress. Each self-locks and must be called with no other client mutex
	// held (Reactor contract: the wasm reactor dispatches inbound frames between checks). The public
	// IsConnected() above is the third of them.
	bool HelloArrived() const;
	bool SessionGateOpen() const;

	// From an inbound callback: fail in-flight reqs, record error, mark disconnected. No reactor stop.
	void FailConnectionAndAllInFlightRequests(const std::string &reason);
	// Stops the reactor (idempotent). From the owner (dtor/Close) only, never an inbound callback.
	void ShutdownFromOwner(const std::string &reason);
	void OnMessage(const std::string &bytes, bool binary);
	void OnClose(const std::string &reason);

	// Re-establish after a drop: reset handshake, rebuild reactor, re-run HELLO. Lazy from
	// AwaitReadySessionAndSendRequest. Throws if the peer is unusable, so the caller sees why.
	void Reconnect();

	// Why the completed handshake is unusable (timeout, HELLO_ERR, protocol mismatch), "" if it is fine.
	// `hello_arrived` is what WaitProgress returned. Shared by Connect and Reconnect.
	std::string HandshakeRejectionReason(bool hello_arrived) const;

	void SendFrame(const std::string &frame_bytes);

	// Snapshot the current reactor as a strong ref so a caller (Send/WaitProgress/Poll) keeps it alive
	// while Reconnect concurrently swaps in a new one — avoids the use-after-free of a bare pointer.
	std::shared_ptr<Reactor> CurrentReactor() const;

	WsClientOptions opts_;
	// reactor_ is hot-swapped across a reconnect and read concurrently by compute pthreads under
	// threads>1. reactor_mu_ guards the pointer itself; reconnect_mu_ serializes the whole Reconnect
	// body (with a double-checked connected_) so two threads can't both rebuild + spawn service threads.
	mutable std::mutex reactor_mu_;
	std::mutex reconnect_mu_;
	std::shared_ptr<Reactor> reactor_;

	// Deliberately outside ConnState: a one-way latch, set by the owner (dtor/Close) and read on the
	// send path without a lock. It answers "is this object being torn down", which is true regardless
	// of what the handshake state says.
	std::atomic<bool> stopping_ {false};
	std::atomic<uint32_t> next_req_id_ {1};
	// Transport channel epoch at last (re)connect; if the reactor moved past it the host reset us (wasm only).
	// Atomic rather than part of ConnState: it is compared against Reactor::Epoch(), which is read
	// without this lock, so pairing it with the handshake fields would buy no extra consistency.
	std::atomic<uint64_t> connected_epoch_ {0};

	// The reactor signals progress and WaitProgress re-checks the state under this one lock.
	mutable std::mutex state_mu_;
	ConnState state_;

	std::mutex reqs_mu_;
	std::unordered_map<uint32_t, std::shared_ptr<RequestState>> reqs_;

	std::mutex push_handler_mu_;
	PushHandler push_handler_;
};

} // namespace n6k
} // namespace duckdb
