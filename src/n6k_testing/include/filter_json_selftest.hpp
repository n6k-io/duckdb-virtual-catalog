#pragma once

// `n6k_testing_filter_json_fidelity()` — a diagnostic table function covering the TableFilter -> JSON
// wire serializer in src/common/include/filter_json.hpp. TableFilterSets are constructible in plain
// C++ with no bound plan, so the serializer's fidelity rules (nested OR, OPTIONAL_FILTER unwrapping,
// and all-or-nothing on anything unrenderable) are testable without a server or a network.
// Emits one row per case: (case_name, json, exact). See test/sql/filter_json.test.

#include "duckdb.hpp"

namespace duckdb {

void RegisterN6kTestingFilterJsonFidelity(ExtensionLoader &loader);

} // namespace duckdb
