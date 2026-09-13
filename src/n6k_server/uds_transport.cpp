#include "uds_transport.hpp"

#include "duckdb/common/exception.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace duckdb {
namespace n6k {

// Bound a corrupt/hostile length prefix; 1 GiB is far above any real frame.
static constexpr uint32_t MAX_FRAME_BYTES = 1u << 30;

UdsConnection::~UdsConnection() {
	Close();
}

void UdsConnection::Close() {
	if (fd_ >= 0) {
		::close(fd_);
		fd_ = -1;
	}
}

void UdsConnection::Connect(const std::string &path) {
	if (path.size() >= sizeof(sockaddr_un::sun_path)) {
		throw IOException("n6k_server: socket path too long (%lu bytes, max %lu): %s",
		                  static_cast<uint64_t>(path.size()), static_cast<uint64_t>(sizeof(sockaddr_un::sun_path) - 1),
		                  path);
	}
	int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		throw IOException("n6k_server: socket() failed: %s", std::strerror(errno));
	}
#ifdef SO_NOSIGPIPE
	// macOS: suppress SIGPIPE at socket level (Linux uses MSG_NOSIGNAL per send).
	int on = 1;
	::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	sockaddr_un addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	std::memcpy(addr.sun_path, path.c_str(), path.size());

	int rc;
	do {
		rc = ::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
	} while (rc < 0 && errno == EINTR);
	if (rc < 0) {
		int err = errno;
		::close(fd);
		throw IOException("n6k_server: connect(%s) failed: %s", path, std::strerror(err));
	}
	fd_ = fd;
}

void UdsConnection::AdoptBlockingDupOfFd(int host_fd) {
	if (host_fd < 0) {
		throw IOException("n6k_server: cannot adopt file descriptor %d", host_fd);
	}
	// dup() so our copy survives until this connection drops, regardless of when the host closes its
	// own — same ownership rule the client-side FdHub uses for a host-supplied socket.
	int fd = ::dup(host_fd);
	if (fd < 0) {
		throw IOException("n6k_server: dup(%d) failed: %s", host_fd, std::strerror(errno));
	}
#ifdef SO_NOSIGPIPE
	// macOS: suppress SIGPIPE at socket level (Linux uses MSG_NOSIGNAL per send). Without this a
	// write to a hung-up peer kills the host process, which for an adopted fd is not ours to kill.
	int on = 1;
	::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	// ReadExactOrThrow/WriteFrame retry EINTR but not EAGAIN, so the fd has to be blocking. A host that
	// handed the socket to an event loop first will have marked its copy non-blocking.
	int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags >= 0 && (flags & O_NONBLOCK) != 0) {
		if (::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) {
			int err = errno;
			::close(fd);
			throw IOException("n6k_server: could not clear O_NONBLOCK on adopted fd %d: %s", host_fd,
			                  std::strerror(err));
		}
	}
	Close();
	fd_ = fd;
}

// Read exactly n bytes; false on clean EOF before any byte, throws on partial/hard error.
static bool ReadExactOrThrow(int fd, char *buf, size_t n) {
	size_t got = 0;
	while (got < n) {
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r == 0) {
			if (got == 0) {
				return false;
			}
			throw IOException("n6k_server: peer closed mid-frame (%lu of %lu bytes)", static_cast<uint64_t>(got),
			                  static_cast<uint64_t>(n));
		}
		if (r < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("n6k_server: read() failed: %s", std::strerror(errno));
		}
		got += static_cast<size_t>(r);
	}
	return true;
}

bool UdsConnection::WaitReadable(int timeout_ms) {
	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;
	pfd.revents = 0;
	int rc;
	do {
		rc = ::poll(&pfd, 1, timeout_ms);
	} while (rc < 0 && errno == EINTR);
	if (rc < 0) {
		throw IOException("n6k_server: poll() failed: %s", std::strerror(errno));
	}
	// POLLHUP/POLLERR also mean "readable" — ReadFrame will observe the EOF/error.
	return rc > 0 && (pfd.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
}

bool UdsConnection::ReadFrame(std::string &out) {
	char len_buf[4];
	if (!ReadExactOrThrow(fd_, len_buf, 4)) {
		return false;
	}
	const unsigned char *p = reinterpret_cast<const unsigned char *>(len_buf);
	uint32_t len = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
	               (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
	if (len > MAX_FRAME_BYTES) {
		throw IOException("n6k_server: frame length %u exceeds limit %u", len, MAX_FRAME_BYTES);
	}
	out.resize(len);
	if (len > 0 && !ReadExactOrThrow(fd_, &out[0], len)) {
		throw IOException("n6k_server: peer closed before frame body (%u bytes expected)", len);
	}
	return true;
}

void UdsConnection::WriteFrame(const std::string &frame) {
	// Length prefix + body in one contiguous buffer: one syscall, and the partial-send loop below
	// resumes mid-frame. Not atomic — interleaving is impossible only because ServeReactor's writer
	// thread is the sole caller (serve_transport.hpp).
	std::string buf;
	buf.reserve(4 + frame.size());
	uint32_t len = static_cast<uint32_t>(frame.size());
	buf.push_back(static_cast<char>((len >> 24) & 0xff));
	buf.push_back(static_cast<char>((len >> 16) & 0xff));
	buf.push_back(static_cast<char>((len >> 8) & 0xff));
	buf.push_back(static_cast<char>(len & 0xff));
	buf.append(frame);

	size_t sent = 0;
	while (sent < buf.size()) {
#ifdef MSG_NOSIGNAL
		ssize_t w = ::send(fd_, buf.data() + sent, buf.size() - sent, MSG_NOSIGNAL);
#else
		ssize_t w = ::write(fd_, buf.data() + sent, buf.size() - sent);
#endif
		if (w < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("n6k_server: write() failed: %s", std::strerror(errno));
		}
		sent += static_cast<size_t>(w);
	}
}

} // namespace n6k
} // namespace duckdb
