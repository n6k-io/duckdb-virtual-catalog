#pragma once

#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"
#include "n6k_yyjson_util.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k_body {

using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_write;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val_mut_copy;

// Attaches pre-serialized JSON `src` under `key`, or `fallback` when `src` is empty (the key is absent).
//
// Malformed `src` throws. It must not be silently dropped: these bodies carry `filters`, so substituting an
// empty value would send a request with NO filter and return rows the query excluded -- the same wrong-answer
// failure that FilterSerializeResult::all_exact exists to prevent on the serializing side (see
// src/common/include/filter_json.hpp). Every `src` here is produced by our own serializer, so a parse failure
// means a bug upstream, not bad user input.
inline void AddParsedJsonOrThrowFreeingDoc(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *key,
                                           const std::string &src, const char *fallback) {
	const char *data = src.empty() ? fallback : src.c_str();
	size_t len = src.empty() ? std::strlen(fallback) : src.size();
	auto *idoc = yyjson_read(data, len, 0);
	if (!idoc) {
		yyjson_mut_doc_free(doc);
		throw InvalidInputException("n6k: request body field '%s' is not valid JSON: %s", key, src);
	}
	yyjson_mut_obj_add_val(doc, root, key, yyjson_val_mut_copy(doc, yyjson_doc_get_root(idoc)));
	yyjson_doc_free(idoc);
}

inline std::string SchemaTable(const std::string &schema, const std::string &table) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string Sql(const std::string &sql) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "sql", sql.c_str(), sql.size());
	return n6k::SerializeJsonDocAndFree(doc);
}

// OP_AGGREGATE. `group_by` names plain columns; `aggregates_json` is a pre-serialized array of
// {"fn":...,"col":...}. Result columns are positional: every group key, then every aggregate.
inline std::string Aggregate(const std::string &schema, const std::string &table,
                             const std::vector<std::string> &columns, const std::string &filters_json,
                             const std::vector<std::string> &group_by, const std::string &aggregates_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());

	auto *cols = duckdb_yyjson::yyjson_mut_arr(doc);
	for (auto &c : columns) {
		duckdb_yyjson::yyjson_mut_arr_add_strncpy(doc, cols, c.c_str(), c.size());
	}
	yyjson_mut_obj_add_val(doc, root, "columns", cols);

	auto *groups = duckdb_yyjson::yyjson_mut_arr(doc);
	for (auto &g : group_by) {
		duckdb_yyjson::yyjson_mut_arr_add_strncpy(doc, groups, g.c_str(), g.size());
	}
	yyjson_mut_obj_add_val(doc, root, "group_by", groups);

	AddParsedJsonOrThrowFreeingDoc(doc, root, "aggregates", aggregates_json, "[]");
	if (!filters_json.empty()) {
		AddParsedJsonOrThrowFreeingDoc(doc, root, "filters", filters_json, "[]");
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string Rpc(const std::string &function, const std::string &args_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "function", function.c_str(), function.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "args", args_json, "[]");
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string CreateTable(const std::string &schema, const std::string &name, const std::string &columns_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "name", name.c_str(), name.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "columns", columns_json, "[]");
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string AlterTable(const std::string &schema, const std::string &table, const std::string &kind,
                              const std::string &details_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());
	yyjson_mut_obj_add_strncpy(doc, root, "kind", kind.c_str(), kind.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "details", details_json, "{}");
	return n6k::SerializeJsonDocAndFree(doc);
}

} // namespace n6k_body
} // namespace duckdb
