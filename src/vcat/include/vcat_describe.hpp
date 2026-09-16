#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

// A single-row typed descriptor for one table. `function_name` is bridge_table_describe or
// provider_table_describe -- each extension owns its own name, so both can be loaded into one
// process. It also names the function in the bind-time error text.
void RegisterTableDescribeFunction(ExtensionLoader &loader, const string &function_name);

} // namespace duckdb
