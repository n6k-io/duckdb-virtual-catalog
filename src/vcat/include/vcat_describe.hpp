#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

// A single-row typed descriptor for one table. Each extension passes its own `function_name`, so both
// can be loaded into one process. It also names the function in the bind-time error text.
void RegisterTableDescribeFunction(ExtensionLoader &loader, const string &function_name);

} // namespace duckdb
