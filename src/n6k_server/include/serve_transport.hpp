#pragma once

#include <string>

namespace duckdb {
namespace n6k {

// What ServeReactor needs from a connection, and nothing more — the reactor only ever waits,
// reads one frame, or writes one frame. Keeping that surface to three calls is what lets the
// same reactor sit behind a Unix socket it dialed (UdsConnection) and a WebSocket a client
// dialed into (WsServeTransport), with the framing question owned entirely by the transport.
class ServeTransport {
public:
	virtual ~ServeTransport() = default;

	// Block up to timeout_ms for a frame to be available; false on timeout, so the reader loop
	// gets a chance to notice an interrupt on an otherwise idle peer.
	virtual bool WaitReadable(int timeout_ms) = 0;

	// Read one whole frame; false on orderly EOF (which ends the session), throws on hard error.
	virtual bool ReadFrame(std::string &out) = 0;

	// Write one whole frame; throws on error. Only ever called from the reactor's writer thread,
	// so implementations need no send-side lock of their own.
	virtual void WriteFrame(const std::string &frame) = 0;
};

} // namespace n6k
} // namespace duckdb
