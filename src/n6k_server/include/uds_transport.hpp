#pragma once

#include "serve_transport.hpp"

#include <string>

namespace duckdb {
namespace n6k {

// Blocking UDS connection; frames are [4-byte BE length][frame]. Socket client, protocol server.
class UdsConnection : public ServeTransport {
public:
	UdsConnection() = default;
	~UdsConnection() override;

	UdsConnection(const UdsConnection &) = delete;
	UdsConnection &operator=(const UdsConnection &) = delete;

	void Connect(const std::string &path);

	// Take over an already-connected stream fd (e.g. one end of a host's socketpair). Duplicates it,
	// so the host may close its own copy as soon as this returns.
	void AdoptBlockingDupOfFd(int host_fd);

	bool WaitReadable(int timeout_ms) override;
	bool ReadFrame(std::string &out) override;
	void WriteFrame(const std::string &frame) override;

	void Close();
	bool IsOpen() const {
		return fd_ >= 0;
	}

private:
	int fd_ = -1;
};

} // namespace n6k
} // namespace duckdb
