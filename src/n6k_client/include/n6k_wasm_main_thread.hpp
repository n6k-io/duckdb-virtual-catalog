#pragma once

// Run n6k's JS glue (globalThis.n6k.*) on the main runtime thread, callable from any thread.
//
// The glue is registered only on the main duckdb worker's JS global scope. Each emscripten pthread
// has its OWN JS global scope with no `n6k`, so a direct emscripten_run_script("n6k.*") on a compute
// pthread throws "n6k is not defined". Under threads>1 duckdb dispatches n6k control-plane work
// (ATTACH -> CatalogSession::Create's vsockAttach, DETACH -> the reactor Stop's vsockClose) onto
// arbitrary compute pthreads, so those calls must run on the main runtime thread where the glue
// lives. emscripten_sync_run_in_main_runtime_thread runs the function inline when already on the
// main thread (the threads=1 case — no round-trip) and proxies it there otherwise.

#ifdef WASM_LOADABLE_EXTENSIONS

#include <emscripten.h>
#include <string>

#ifdef WITH_WASM_THREADS
#include <cstring>
#include <emscripten/threading.h>
#endif

namespace duckdb {
namespace n6k {

#ifdef WITH_WASM_THREADS

namespace detail {

struct N6kScriptStringCall {
	const char *js;
	char *out;
	int cap;
};

inline void N6kRunScriptStringOnMain(void *arg) {
	auto *call = static_cast<N6kScriptStringCall *>(arg);
	const char *r = emscripten_run_script_string(call->js);
	if (r && call->cap > 0) {
		std::strncpy(call->out, r, static_cast<size_t>(call->cap - 1));
		call->out[call->cap - 1] = '\0';
	} else if (call->cap > 0) {
		call->out[0] = '\0';
	}
}

inline void N6kRunScriptVoidOnMain(void *arg) {
	emscripten_run_script(static_cast<const char *>(arg));
}

} // namespace detail

// Returns the script's string result (truncated to the internal buffer — n6k glue returns are
// short status strings like "OK-heap"/"OK-sab" or an error message).
inline std::string RunScriptStringOnMain(const std::string &js) {
	char buf[512];
	buf[0] = '\0';
	detail::N6kScriptStringCall call {js.c_str(), buf, static_cast<int>(sizeof(buf))};
	emscripten_sync_run_in_main_runtime_thread(EM_FUNC_SIG_VI, &detail::N6kRunScriptStringOnMain, &call);
	return std::string(buf);
}

inline void RunScriptOnMain(const std::string &js) {
	emscripten_sync_run_in_main_runtime_thread(EM_FUNC_SIG_VI, &detail::N6kRunScriptVoidOnMain,
	                                           const_cast<char *>(js.c_str()));
}

#else

inline std::string RunScriptStringOnMain(const std::string &js) {
	const char *r = emscripten_run_script_string(js.c_str());
	return r ? std::string(r) : std::string();
}

inline void RunScriptOnMain(const std::string &js) {
	emscripten_run_script(js.c_str());
}

#endif

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
