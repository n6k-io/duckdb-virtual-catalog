#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "filter_json.hpp"
#include "n6k_str_utils.hpp"

namespace duckdb {

// Filter JSON uses the n6k tuple-array encoder (filter_json.hpp) in its Wire dialect. The
// virtual_catalog_provider extension carries its own copy of the encoder and speaks the Provider
// dialect -- shared shape, not a shared dialect, which is why both name the one they speak.
static string BuildScanUrlQueryString(ArrowStreamParameters &parameters, const ArrowSchema &schema) {
	string query;

	auto &cols = parameters.projected_columns.columns;
	if (!cols.empty()) {
		query += "columns=";
		for (idx_t i = 0; i < cols.size(); i++) {
			if (i > 0) {
				query += ",";
			}
			query += n6k::UrlEncode(cols[i]);
		}
	}

	if (parameters.filters) {
		vector<string> column_names;
		column_names.reserve(schema.n_children);
		for (idx_t i = 0; i < (idx_t)schema.n_children; i++) {
			column_names.emplace_back(schema.children[i]->name);
		}
		auto options = n6k_filter_json::FilterJsonOptions::Wire();
		n6k_filter_json::FilterSerializeResult result;
		auto filters_json = n6k_filter_json::SerializeFilters(
		    *parameters.filters, parameters.projected_columns.filter_to_col, column_names, options, result);
		if (!result.all_exact) {
			n6k_filter_json::ThrowUnrenderableFilter("n6k scan", result.first_unsupported);
		}
		if (!filters_json.empty()) {
			if (!query.empty()) {
				query += "&";
			}
			query += "filters=" + n6k::UrlEncode(filters_json);
		}
	}

	return query;
}

} // namespace duckdb
