#ifdef WASM_LOADABLE_EXTENSIONS

// WsClient seam. Two backings, chosen per-catalog at attach time:
//   direct (coi/threads): the reactor registers its heap rings with the single shared n6k I/O thread
//                         (n6k_io_thread.hpp), which across ALL catalogs is the sole consumer of RX
//                         ring1 (drains each frame -> on_frame_ -> WsClient::OnMessage -> Push, and
//                         wakes a parked WaitUntil) AND the sole producer of TX ring0. Send just
//                         enqueues (non-blocking) and rings the doorbell; the I/O thread TryWrites, so
//                         no producer ever blocks the shared thread. No JS on any of these paths, so
//                         the I/O thread is safe with no globalThis.n6k. Requires WITH_WASM_THREADS
//                         and a non-zero ring_ptr.
//   glue (fallback):      no I/O thread; inbound pulled inline by WaitUntil/Poll over the JS vsock
//                         glue (globalThis.n6k, keyed by catalog). Used whenever JS chose a
//                         standalone SharedArrayBuffer (non-threaded / no capturedWasmMemory).

#include "n6k_io_thread.hpp" // shared I/O thread singleton (compiled out unless WITH_WASM_THREADS)
#include "n6k_str_utils.hpp"
#include "n6k_vsock_ring.hpp"       // direct-path ring accessor (compiled out unless WITH_WASM_THREADS)
#include "n6k_wasm_main_thread.hpp" // RunScriptOnMain
#include "ws_client.hpp"            // WsClientOptions
#include "ws_reactor.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <emscripten.h>
#include <mutex>
#include <string>
#include <vector>

#ifdef WITH_WASM_THREADS
#include <condition_variable>
#include <thread>
#endif

namespace duckdb {
namespace n6k {

namespace {

class WasmReactor : public Reactor {
public:
	WasmReactor(std::string catalog, uintptr_t ring_ptr) : catalog_(std::move(catalog)) {
#ifdef WITH_WASM_THREADS
		if (ring_ptr != 0) {
			direct_ = true;
			MakeVsockChannelRings(ring_ptr, ring0_, ring1_); // ring0 = SEND (duckdb->ws), ring1 = RECV
		}
#else
		(void)ring_ptr;
#endif
	}
	~WasmReactor() override {
		Stop();
	}

	void Start(FrameFn on_frame, CloseFn on_close) override {
		on_frame_ = std::move(on_frame);
		on_close_ = std::move(on_close);
#ifdef WITH_WASM_THREADS
		if (direct_) {
			// Register ring1 with the shared I/O thread; it becomes ring1's sole consumer. Callbacks
			// are set above first, and Unregister (Stop/Abandon) quiesces the I/O thread off this ring
			// before they're cleared, so it can never call a stale callback.
			N6kIoThread::Registration reg;
			reg.ring0 = ring0_;
			reg.ring1 = ring1_;
			reg.pinned_epoch = ring1_.Epoch();
			reg.on_frame = on_frame_;
			reg.on_progress = [this] {
				NotifyProgress();
			};
			reg.on_close = [this](const char *reason) {
				FireCloseOnce(reason);
			};
			handle_ = N6kIoThread::Instance().Register(std::move(reg));
			registered_ = true;
		}
#endif
	}

	// Direct path: the shared I/O thread delivers frames. Glue path: the requester drives DrainInbound.
	bool SelfDelivers() const override {
#ifdef WITH_WASM_THREADS
		return direct_;
#else
		return false;
#endif
	}

	void Send(const std::string &frame) override {
		if (stopped_.load()) {
			return;
		}
#ifdef WITH_WASM_THREADS
		if (direct_) {
			// Enqueue for the shared I/O thread (ring0's sole producer); never touches a ring, never
			// blocks. Reject an unfittable frame here on the caller's thread — matching the old
			// BlockingWrite->TryWrite throw — so the oversized case never reaches the shared thread.
			if (N6K_RING_LEN_BYTES + frame.size() > N6K_RING_CAPACITY) {
				throw IOException("n6k vsock: frame %llu B + prefix exceeds ring capacity %u",
				                  static_cast<unsigned long long>(frame.size()), N6K_RING_CAPACITY);
			}
			N6kIoThread::Instance().Enqueue(handle_, frame);
			return;
		}
#endif
		// Stage in a heap buffer for the JS glue; it copies synchronously so we free after.
		auto *buf = static_cast<uint8_t *>(std::malloc(frame.size() ? frame.size() : 1));
		if (!buf) {
			return;
		}
		if (!frame.empty()) {
			std::memcpy(buf, frame.data(), frame.size());
		}
		std::string js = "n6k.vsockSend('" + EscapeSingleQuotedJsLiteral(catalog_) + "', " +
		                 std::to_string(reinterpret_cast<uintptr_t>(buf)) + ", " + std::to_string(frame.size()) +
		                 ", HEAPU8)";
		emscripten_run_script(js.c_str());
		std::free(buf);
	}

	bool WaitUntil(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) override {
#ifdef WITH_WASM_THREADS
		if (direct_) {
			// The shared I/O thread drains ring1 and NotifyProgress()es; just park until a delivered
			// frame satisfies the predicate, the reactor stops, or the deadline passes. `ready`
			// self-locks client state and is safe under progress_mu_ (the I/O thread never holds
			// progress_mu_ while locking client state — it Push()es, then NotifyProgress separately).
			std::unique_lock<std::mutex> lk(progress_mu_);
			progress_cv_.wait_until(lk, deadline, [&] { return ready() || stopped_.load(); });
			return ready();
		}
#endif
		for (;;) {
			DrainInbound();
			if (ready()) {
				return true;
			}
			if (stopped_.load()) {
				return ready();
			}
			auto now = std::chrono::steady_clock::now();
			if (now >= deadline) {
				return ready();
			}
			auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
			std::string js = "n6k.vsockWait('" + EscapeSingleQuotedJsLiteral(catalog_) + "', " +
			                 std::to_string(static_cast<long long>(ms)) + ")";
			emscripten_run_script(js.c_str());
		}
	}

	void PollAndDispatchInbound() override {
#ifdef WITH_WASM_THREADS
		if (direct_) {
			// The shared I/O thread is the sole ring1 consumer; draining here too would violate SPSC.
			return;
		}
#endif
		if (!stopped_.load()) {
			DrainInbound();
		}
	}

	uint64_t Epoch() override {
#ifdef WITH_WASM_THREADS
		if (direct_) {
			return ring0_.Epoch();
		}
#endif
		std::string js = "n6k.vsockEpoch('" + EscapeSingleQuotedJsLiteral(catalog_) + "')";
		const char *r = emscripten_run_script_string(js.c_str());
		long e = r ? std::atol(r) : 0;
		// -1 (channel gone) reads as "no epoch"; clamp negatives to 0.
		return e > 0 ? static_cast<uint64_t>(e) : 0;
	}

	void Stop() override {
		if (torn_down_.exchange(true)) {
			return; // once-only teardown (stopped_ may already be set by the shared I/O thread on a drop)
		}
		stopped_.store(true);
#ifdef WITH_WASM_THREADS
		if (direct_) {
			// Unregister BLOCKS until the shared I/O thread acks it will never touch ring1 again (the
			// quiesce barrier that replaces the old per-reactor join). Only then is it safe to Close +
			// free: the I/O thread is provably off this ring before vsockClose posts n6k-vsock-detach
			// -> pump-ring-released -> capturedWasmFree. Close still signals the pump (CLOSED) as before.
			if (registered_) {
				N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
				registered_ = false;
			}
			ring0_.Close();
			ring1_.Close();
		}
#endif
		// JS lifecycle (both paths): vsockClose closes the duckdb-worker endpoint and posts
		// n6k-vsock-detach so the driver retires the pump ws-worker and frees the heap ring. No
		// on_close_ here — ShutdownFromOwner already called FailConnectionAndAllInFlightRequests, and on the direct
		// path the shared I/O thread fires on_close_ for a genuine drop. Matches the native reactor's Stop. Run on the
		// main runtime thread: under threads>1 DETACH can reach Stop on a compute pthread, whose JS scope has no
		// globalThis.n6k (see n6k_wasm_main_thread.hpp).
		std::string js = "n6k.vsockClose('" + EscapeSingleQuotedJsLiteral(catalog_) + "')";
		RunScriptOnMain(js);
	}

	// Reconnect discard: retire this generation but leave the channel open for reuse (must NOT close
	// the ring or vsockClose — Reconnect re-attaches the same ring_ptr). Unregister quiesces the shared
	// I/O thread off ring1 before nulling callbacks, so it can never call a cleared callback and the
	// new generation can re-register the SAME ring with no overlapping consumer.
	void RetireKeepingChannelOpen() override {
		if (torn_down_.exchange(true)) {
			return;
		}
		stopped_.store(true);
#ifdef WITH_WASM_THREADS
		if (direct_ && registered_) {
			N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
			registered_ = false;
		}
#endif
		on_frame_ = nullptr;
		on_close_ = nullptr;
	}

private:
#ifdef WITH_WASM_THREADS
	void NotifyProgress() {
		std::lock_guard<std::mutex> lk(progress_mu_);
		progress_cv_.notify_all();
	}

	// Fire on_close_ at most once (drop or host reset), mark stopped, and wake a parked WaitUntil.
	void FireCloseOnce(const char *reason) {
		stopped_.store(true);
		if (!close_fired_.exchange(true) && on_close_) {
			on_close_(reason);
		}
		NotifyProgress();
	}
#endif

	// Glue path only: pull one frame into recv_buf_. Returns the glue protocol (>=0 len, -1 empty,
	// -3 empty+closed, < -3 frame > cap not consumed).
	long RecvRaw(uint8_t *ptr, size_t cap) {
		std::string js = "n6k.vsockRecv('" + EscapeSingleQuotedJsLiteral(catalog_) + "', " +
		                 std::to_string(reinterpret_cast<uintptr_t>(ptr)) + ", " + std::to_string(cap) + ", HEAPU8)";
		const char *r = emscripten_run_script_string(js.c_str());
		return r ? std::atol(r) : -1;
	}

	// Glue path only (direct path drains on the service thread instead).
	void DrainInbound() {
		if (recv_buf_.size() < 65536) {
			recv_buf_.resize(65536);
		}
		for (;;) {
			for (;;) {
				long n = RecvRaw(recv_buf_.data(), recv_buf_.size());
				if (n == -1) {
					return; // ring empty, still open
				}
				if (n == -3) {
					// Channel closed: fail in-flight requests fast instead of blocking to the deadline.
					stopped_.store(true);
					if (on_close_) {
						on_close_("socket closed");
					}
					return;
				}
				if (n < -1) {
					recv_buf_.resize(static_cast<size_t>(-n)); // frame > capacity; grow to exact size, retry
					continue;
				}
				std::string frame(reinterpret_cast<char *>(recv_buf_.data()), static_cast<size_t>(n));
				if (on_frame_) {
					on_frame_(frame, true);
				}
				break;
			}
		}
	}

	std::string catalog_;
	FrameFn on_frame_;
	CloseFn on_close_;
	std::atomic<bool> stopped_ {false};
	std::atomic<bool> torn_down_ {false}; // once-only gate for the Stop/Abandon teardown body
	std::vector<uint8_t> recv_buf_;       // glue path RX staging (the shared I/O thread uses its own buffer)
#ifdef WITH_WASM_THREADS
	bool direct_ = false;
	VsockRing ring0_;                // duckdb->ws (SEND); the shared I/O thread is its sole producer while registered
	VsockRing ring1_;                // ws->duckdb (RECV); the shared I/O thread is its sole consumer while registered
	N6kIoThread::Handle handle_ = 0; // registration with the shared I/O thread (valid while registered_)
	bool registered_ = false;
	std::mutex progress_mu_;
	std::condition_variable progress_cv_;   // a parked WaitUntil is notified after each delivered frame
	std::atomic<bool> close_fired_ {false}; // on_close_ fires at most once per generation
#endif
};

} // namespace

std::unique_ptr<Reactor> CreateReactor(const WsClientOptions &opts) {
	// Keyed by the LOCAL alias (channel_key); fall back to opts.catalog for legacy callers.
	// ring_ptr != 0 (coi/threads build only) selects the direct-ring path.
	return std::unique_ptr<Reactor>(
	    new WasmReactor(opts.channel_key.empty() ? opts.catalog : opts.channel_key, opts.ring_ptr));
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
