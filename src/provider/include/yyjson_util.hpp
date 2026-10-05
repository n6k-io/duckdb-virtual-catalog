#pragma once

#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"

#include <cstdlib>
#include <string>

namespace duckdb {
namespace vcat {

// Caller must set the document root first.
inline std::string SerializeJsonDocAndFree(duckdb_yyjson::yyjson_mut_doc *doc) {
	size_t len = 0;
	auto *json = duckdb_yyjson::yyjson_mut_write(doc, duckdb_yyjson::YYJSON_WRITE_ALLOW_INF_AND_NAN, &len);
	if (!json) {
		duckdb_yyjson::yyjson_mut_doc_free(doc);
		throw InvalidInputException("virtual_catalog_provider: a value could not be written as JSON");
	}
	std::string result(json, len);
	free(json);
	duckdb_yyjson::yyjson_mut_doc_free(doc);
	return result;
}

} // namespace vcat
} // namespace duckdb
