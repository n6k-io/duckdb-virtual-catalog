#pragma once

#include "n6k_protocol_generated.hpp"

#include <string>

namespace duckdb {
namespace n6k {

std::string UrlDecode(const std::string &in);

std::string UrlEncode(const std::string &in);

std::string EscapeSingleQuotedJsLiteral(const std::string &s);

std::string UrlQueryToOpScanBody(const std::string &schema, const std::string &table, const std::string &query);

} // namespace n6k
} // namespace duckdb
