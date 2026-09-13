#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "ws_transport.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace duckdb {
namespace n6k {

// Owns ONE socket fd and fans it out to the catalog sessions riding it.
//
// A host that hands the same `wsFd` to several ATTACHes is handing out several views of ONE socket.
// Giving each session its own reader would race them on read(): the byte stream has no per-session
// boundaries, so two blocking readers split frames between themselves at arbitrary points and both
// see garbage. So exactly one hub per fd owns the reader, and it demultiplexes by the protocol's `ns`
// key — the same routing key the server stamps every session-scoped response with.
//
// Hubs are keyed by the caller's fd NUMBER in a process-global registry, so a host must not close and
// recycle an fd number while an ATTACH still rides it.
class FdHub {
public:
	using MessageFn = Transport::MessageFn;
	using CloseFn = Transport::CloseFn;

	// The hub owning `host_fd`, creating it (and dup()ing the fd) on first use.
	static std::shared_ptr<FdHub> Acquire(int host_fd);

	~FdHub();
	FdHub(const FdHub &) = delete;
	FdHub &operator=(const FdHub &) = delete;

	// Register a session's callbacks under its `ns`. The reader starts on the first subscriber, so no
	// frame can arrive before anyone is listening. ns == 0 is the untagged/sole session.
	void BindSessionAndStartReader(uint64_t ns, MessageFn on_message, CloseFn on_close);
	// Drop a session. Its on_close does NOT fire — the owner asked for this.
	void Unsubscribe(uint64_t ns);

	// Serialized: every session has its own writer thread.
	bool SendLengthPrefixed(const std::string &frame);

	size_t SubscriberCount();

private:
	explicit FdHub(int host_fd);

	void ReaderLoop();
	void DispatchInbound(const std::string &frame);
	void NotifySubscribersOfClose(const std::string &reason);
	void Shutdown();

	struct Subscriber {
		MessageFn on_message;
		CloseFn on_close;
	};

	// The caller's fd number — the registry key, NOT the dup we read from.
	const int host_fd_;
	int fd_ = -1;
	// Written to so poll() wakes immediately on Shutdown instead of waiting out a timeout. Using
	// shutdown() to unblock the read would tear down the shared socket for every other session.
	int wake_r_ = -1;
	int wake_w_ = -1;

	std::mutex subs_mu_;
	std::map<uint64_t, std::shared_ptr<Subscriber>> subs_;
	bool reader_started_ = false;

	std::mutex write_mu_;
	std::thread reader_;
	std::atomic<bool> stopping_ {false};
};

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
