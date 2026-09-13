#include "ws_serve_transport.hpp"

#include "duckdb/common/exception.hpp"

#include "ixwebsocket/IXWebSocket.h"

#include <chrono>
#include <utility>

namespace duckdb {
namespace n6k {

WsServeTransport::WsServeTransport(ix::WebSocket &ws) : ws_(ws) {
}

void WsServeTransport::EnqueueFrameUnlessClosed(std::string frame) {
	{
		std::lock_guard<std::mutex> lk(mu_);
		// Dropped rather than queued: nothing will ever read it, and queueing after close would
		// keep ReadFrame from reporting the EOF the reactor is waiting for.
		if (closed_) {
			return;
		}
		in_q_.push_back(std::move(frame));
	}
	cv_.notify_all();
}

void WsServeTransport::CloseInbound() {
	{
		std::lock_guard<std::mutex> lk(mu_);
		closed_ = true;
	}
	cv_.notify_all();
}

bool WsServeTransport::WaitReadable(int timeout_ms) {
	std::unique_lock<std::mutex> lk(mu_);
	if (!in_q_.empty() || closed_) {
		return true;
	}
	cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] { return !in_q_.empty() || closed_; });
	return !in_q_.empty() || closed_;
}

bool WsServeTransport::ReadFrame(std::string &out) {
	std::unique_lock<std::mutex> lk(mu_);
	cv_.wait(lk, [&] { return !in_q_.empty() || closed_; });
	// Queued frames win over a close: a client that sends its last request and immediately
	// disconnects must still have that request answered, not dropped.
	if (in_q_.empty()) {
		return false;
	}
	out = std::move(in_q_.front());
	in_q_.pop_front();
	return true;
}

void WsServeTransport::WriteFrame(const std::string &frame) {
	if (!ws_.sendBinary(frame).success) {
		throw IOException("n6k_serve_http: sending a %llu byte frame to the client failed",
		                  static_cast<uint64_t>(frame.size()));
	}
}

} // namespace n6k
} // namespace duckdb
