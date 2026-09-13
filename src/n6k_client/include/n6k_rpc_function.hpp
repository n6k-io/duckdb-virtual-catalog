#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <memory>

namespace duckdb {

class CatalogSession;

void RegisterN6kRpc(ExtensionLoader &loader);

TableFunction MakeN6kCatalogRpcFunction(const string &base_url, const string &function_name,
                                        std::shared_ptr<CatalogSession> session = nullptr);
TableFunction MakeN6kCatalogExecFunction(const string &base_url, std::shared_ptr<CatalogSession> session);
TableFunction MakeN6kCatalogQueryFunction(const string &base_url, std::shared_ptr<CatalogSession> session);

} // namespace duckdb
