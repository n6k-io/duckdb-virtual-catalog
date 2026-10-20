#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterLockdownFunction(ExtensionLoader &loader);

} // namespace duckdb
