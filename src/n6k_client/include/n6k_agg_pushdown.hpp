#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterN6kAggregatePushdown(ExtensionLoader &loader);

} // namespace duckdb
