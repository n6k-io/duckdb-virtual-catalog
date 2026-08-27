#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "filter_json.hpp"

namespace duckdb {

// SQL analogue of filter_json.hpp's TryBuildClauses, sharing its FilterFidelity so the two agree on
// what may be dropped rather than each deciding for itself.
//
// FIDELITY IS LOAD-BEARING here for the same reason it is there: DuckDB removes a fully-pushed
// filter from the plan (optimizer/pushdown/pushdown_get.cpp) and PhysicalTableScan never evaluates
// table_filters, so a clause omitted from this WHERE is not applied anywhere. Only OPTIONAL/
// DYNAMIC/BLOOM filters are droppable -- DuckDB marks those optional exactly when it also keeps a
// FILTER operator above the scan.
static filter_json::FilterFidelity TryRenderFilterSQL(const TableFilter &filter, const string &col_name, string &out) {
	using filter_json::FilterFidelity;
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::IN_FILTER:
		out = filter.ToString(col_name);
		return FilterFidelity::EXACT;
	case TableFilterType::OPTIONAL_FILTER: {
		// Correctness does not depend on this filter, so an unrenderable child is safe to drop.
		auto &opt = filter.Cast<OptionalFilter>();
		if (!opt.child_filter) {
			return FilterFidelity::OPTIONAL_SKIPPED;
		}
		string child_sql;
		auto fidelity = TryRenderFilterSQL(*opt.child_filter, col_name, child_sql);
		if (fidelity == FilterFidelity::UNSUPPORTED) {
			return FilterFidelity::OPTIONAL_SKIPPED;
		}
		out = child_sql;
		return fidelity;
	}
	case TableFilterType::DYNAMIC_FILTER:
	case TableFilterType::BLOOM_FILTER:
		// Join-derived; the join itself enforces the predicate, so skipping only costs rows.
		return FilterFidelity::OPTIONAL_SKIPPED;
	case TableFilterType::CONJUNCTION_AND: {
		// One unrenderable child poisons the conjunction: emitting only the renderable children
		// yields a weaker predicate, and nothing re-applies the rest locally.
		auto &conj = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child : conj.child_filters) {
			string child_sql;
			if (TryRenderFilterSQL(*child, col_name, child_sql) == FilterFidelity::UNSUPPORTED) {
				return FilterFidelity::UNSUPPORTED;
			}
			if (child_sql.empty()) {
				continue;
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += child_sql;
		}
		if (result.empty()) {
			return FilterFidelity::OPTIONAL_SKIPPED;
		}
		out = result;
		return FilterFidelity::EXACT;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conj = filter.Cast<ConjunctionOrFilter>();
		if (conj.child_filters.empty()) {
			return FilterFidelity::UNSUPPORTED;
		}
		// Dropping any branch of a disjunction widens it, so every branch must render exactly --
		// OPTIONAL_SKIPPED is not good enough here.
		string result;
		for (auto &child : conj.child_filters) {
			string child_sql;
			if (TryRenderFilterSQL(*child, col_name, child_sql) != FilterFidelity::EXACT) {
				return FilterFidelity::UNSUPPORTED;
			}
			if (!result.empty()) {
				result += " OR ";
			}
			result += child_sql;
		}
		out = "(" + result + ")";
		return FilterFidelity::EXACT;
	}
	default:
		return FilterFidelity::UNSUPPORTED;
	}
}

static string QuoteDottedName(const string &name) {
	auto parts = StringUtil::Split(name, '.');
	string result;
	for (idx_t i = 0; i < parts.size(); i++) {
		if (i > 0) {
			result += ".";
		}
		result += KeywordHelper::WriteOptionallyQuoted(parts[i]);
	}
	return result;
}

static string BuildBridgeSQL(const string &table_name, ArrowStreamParameters &parameters,
                             const vector<string> &column_names) {
	string columns = "*";
	auto &cols = parameters.projected_columns.columns;
	if (!cols.empty()) {
		columns = "";
		for (idx_t i = 0; i < cols.size(); i++) {
			if (i > 0) {
				columns += ", ";
			}
			columns += KeywordHelper::WriteOptionallyQuoted(cols[i]);
		}
	}

	string quoted_table = QuoteDottedName(table_name);
	string sql = "SELECT " + columns + " FROM " + quoted_table;

	if (parameters.filters) {
		auto &f2c = parameters.projected_columns.filter_to_col;
		vector<string> where_clauses;

		for (auto &entry : parameters.filters->filters) {
			auto filter_idx = entry.first;
			auto &filter = *entry.second;

			auto it = f2c.find(filter_idx);
			if (it == f2c.end() || it->second >= column_names.size()) {
				// No column mapping (a row-id filter, say). It cannot be named in the remote SQL,
				// and dropping it silently would return extra rows.
				filter_json::ThrowUnrenderableFilter("virtual_catalog scan", "?");
			}
			auto col_idx = it->second;

			string col_name = KeywordHelper::WriteOptionallyQuoted(column_names[col_idx]);
			string clause;
			if (TryRenderFilterSQL(filter, col_name, clause) == filter_json::FilterFidelity::UNSUPPORTED) {
				filter_json::ThrowUnrenderableFilter("virtual_catalog scan", column_names[col_idx]);
			}
			if (!clause.empty()) {
				where_clauses.push_back(clause);
			}
		}

		if (!where_clauses.empty()) {
			sql += " WHERE ";
			for (idx_t i = 0; i < where_clauses.size(); i++) {
				if (i > 0) {
					sql += " AND ";
				}
				sql += "(" + where_clauses[i] + ")";
			}
		}
	}

	return sql;
}

} // namespace duckdb
