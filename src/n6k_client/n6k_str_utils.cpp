#include "n6k_str_utils.hpp"

#include "yyjson.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace n6k {

namespace {

using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_add_strncpy;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_write;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val_mut_copy;

} // namespace

std::string UrlEncode(const std::string &in) {
	std::string out;
	out.reserve(in.size() * 3);
	for (unsigned char c : in) {
		if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' ||
		    c == '.' || c == '~') {
			out += static_cast<char>(c);
		} else {
			static const char hex[] = "0123456789ABCDEF";
			out += '%';
			out += hex[c >> 4];
			out += hex[c & 0x0F];
		}
	}
	return out;
}

std::string UrlDecode(const std::string &in) {
	auto hex = [](char h) -> int {
		if (h >= '0' && h <= '9') {
			return h - '0';
		}
		if (h >= 'a' && h <= 'f') {
			return 10 + (h - 'a');
		}
		if (h >= 'A' && h <= 'F') {
			return 10 + (h - 'A');
		}
		return -1;
	};
	std::string out;
	out.reserve(in.size());
	for (size_t i = 0; i < in.size(); ++i) {
		char c = in[i];
		if (c == '+') {
			out.push_back(' ');
		} else if (c == '%' && i + 2 < in.size()) {
			int a = hex(in[i + 1]);
			int b = hex(in[i + 2]);
			if (a < 0 || b < 0) {
				out.push_back(c);
			} else {
				out.push_back(static_cast<char>((a << 4) | b));
				i += 2;
			}
		} else {
			out.push_back(c);
		}
	}
	return out;
}

std::string UrlQueryToOpScanBody(const std::string &schema, const std::string &table, const std::string &query) {
	// Parse `columns=a,b&filters=<urlencoded-json>`; filters embed as a parsed JSON node, not spliced text.
	// Each column name is percent-encoded on its own, so the split on `,` happens BEFORE the decode:
	// decoding first would let a name containing `,`, `&` or `%` tear into names of other columns.
	std::string columns_raw, filters_raw;
	size_t i = 0;
	while (i < query.size()) {
		auto amp = query.find('&', i);
		auto kv = query.substr(i, amp == std::string::npos ? std::string::npos : amp - i);
		auto eq = kv.find('=');
		if (eq != std::string::npos) {
			auto k = kv.substr(0, eq);
			auto v = kv.substr(eq + 1);
			if (k == "columns") {
				columns_raw = v;
			} else if (k == "filters") {
				filters_raw = UrlDecode(v);
			}
		}
		if (amp == std::string::npos) {
			break;
		}
		i = amp + 1;
	}

	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());

	if (!columns_raw.empty()) {
		auto *arr = yyjson_mut_arr(doc);
		size_t j = 0;
		while (j < columns_raw.size()) {
			auto comma = columns_raw.find(',', j);
			auto name = UrlDecode(columns_raw.substr(j, comma == std::string::npos ? std::string::npos : comma - j));
			yyjson_mut_arr_add_strncpy(doc, arr, name.c_str(), name.size());
			if (comma == std::string::npos) {
				break;
			}
			j = comma + 1;
		}
		yyjson_mut_obj_add_val(doc, root, "columns", arr);
	}

	if (!filters_raw.empty()) {
		auto *idoc = yyjson_read(filters_raw.c_str(), filters_raw.size(), 0);
		// Dropping an unparseable filter would send a scan with NO filter and return rows the query
		// excluded -- a wrong answer, not a missing one. The string is built by our own serializer, so a
		// parse failure is a bug on this side; the same rule n6k_body_builders.hpp enforces.
		if (!idoc) {
			yyjson_mut_doc_free(doc);
			throw InvalidInputException("n6k: scan filters are not valid JSON: %s", filters_raw);
		}
		yyjson_mut_obj_add_val(doc, root, "filters", yyjson_val_mut_copy(doc, yyjson_doc_get_root(idoc)));
		yyjson_doc_free(idoc);
	}

	size_t len = 0;
	auto *json = yyjson_mut_write(doc, 0, &len);
	std::string body(json, len);
	free(json);
	yyjson_mut_doc_free(doc);
	return body;
}

std::string EscapeSingleQuotedJsLiteral(const std::string &s) {
	std::string result;
	result.reserve(s.size());
	for (auto c : s) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '\'':
			result += "\\'";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\0':
			result += "\\0";
			break;
		default:
			result += c;
		}
	}
	return result;
}

} // namespace n6k
} // namespace duckdb
