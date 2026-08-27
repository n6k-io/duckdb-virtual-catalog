#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

// Registers vcat_table_describe: a single-row typed descriptor for one table.
void RegisterTableDescribeFunction(ExtensionLoader &loader);

} // namespace duckdb
