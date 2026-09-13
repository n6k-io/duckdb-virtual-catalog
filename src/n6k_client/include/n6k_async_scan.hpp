#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers the async full-scan machinery: an OptimizerExtension that rewrites a full (all-columns,
// no-filter) `n6k_scan` LogicalGet on an attached WebSocket catalog into a custom BLOCKED source
// operator, plus the `n6k_async_scan_stats()` diagnostic table function (parks / wakes counters).
//
// The operator yields its worker thread (SourceResultType::BLOCKED with no task) while waiting for the
// next Arrow frame and is rescheduled by RequestState::SetOnFrame -> interrupt_state.Callback(), so
// concurrent scans overlap instead of each freezing the thread. Both native and the coi/wasm_threads
// bundle take this path (kN6kAsyncScanEnabled, n6k_async_scan.cpp).
void RegisterN6kAsyncScan(ExtensionLoader &loader);
} // namespace duckdb
