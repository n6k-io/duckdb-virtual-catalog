#pragma once

#include "serve_transport.hpp"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>

namespace ix {
class WebSocket;
} // namespace ix

namespace duckdb {
namespace n6k {

// One accepted WebSocket client, as a transport the reactor can pull from.
//
// ixwebsocket PUSHES messages at a callback on its own per-client thread, while ServeReactor
// PULLS. This queue is the adapter between the two: the callback thread calls PushFrame, the
// reactor thread drains it through WaitReadable/ReadFrame. Restructuring the reactor to be
// callback-driven would have meant a second, divergent copy of the routing loop.
//
// Note there is NO length prefix here, unlike UdsConnection — a WebSocket message already IS a
// frame boundary. A pump in front of UdsConnection translates that socket's prefixed stream
// *to* this convention.
class WsServeTransport : public ServeTransport {
public:
	explicit WsServeTransport(ix::WebSocket &ws);

	WsServeTransport(const WsServeTransport &) = delete;
	WsServeTransport &operator=(const WsServeTransport &) = delete;

	bool WaitReadable(int timeout_ms) override;
	bool ReadFrame(std::string &out) override;
	void WriteFrame(const std::string &frame) override;

	// Hand one received frame to the reactor. Called from ixwebsocket's callback thread.
	void EnqueueFrameUnlessClosed(std::string frame);

	// The client went away (Close or Error). Drains what is queued, then reports EOF from
	// ReadFrame, which unwinds the reactor exactly as a closed Unix socket does.
	void CloseInbound();

private:
	ix::WebSocket &ws_;

	std::mutex mu_;
	std::condition_variable cv_;
	std::deque<std::string> in_q_;
	bool closed_ = false;
};

} // namespace n6k
} // namespace duckdb
