#pragma once

// Serializes DuckDB's TableFilterSet to the flat JSON tuple-array the provider path speaks:
//
//   ["col", "op", value]        a clause
//   ["col", "op", value, tag]   the same, with a rebuild tag for a value that crossed as text
//
// Top-level array elements are AND-joined by the consumer. There is no representation for a
// disjunction, so an OR renders UNSUPPORTED.
//
// Fidelity is load-bearing. DuckDB does not re-apply pushed filters after an Arrow scan
// (ArrowScanLocalState::filters is assigned but never read), so whatever is serialized here IS the
// filter: dropping a mandatory clause returns rows the query excluded. Callers must check
// FilterSerializeResult::all_exact and refuse a filter set that did not render.
//
// OPTIONAL_FILTER (and DYNAMIC/BLOOM) are the exception -- DuckDB marks a filter optional exactly
// when it also keeps a FILTER operator above the scan. STRUCT_EXTRACT is the case to watch: pushed
// mandatorily with no FILTER retained.

#include "duckdb.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "yyjson.hpp"

#include <cstring>

namespace duckdb {

namespace filter_json {

using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_add_strncpy;
using duckdb_yyjson::yyjson_mut_arr_append;
using duckdb_yyjson::yyjson_mut_arr_size;
using duckdb_yyjson::yyjson_mut_bool;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_null;
using duckdb_yyjson::yyjson_mut_real;
using duckdb_yyjson::yyjson_mut_sint;
using duckdb_yyjson::yyjson_mut_strncpy;
using duckdb_yyjson::yyjson_mut_uint;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_write;

//! How faithfully a filter rendered to the wire format.
enum class FilterFidelity : uint8_t {
	EXACT,
	//! Omitted, but query correctness does not depend on it (hint filters: a downstream operator
	//! enforces the predicate, so the server returning a superset is still correct).
	OPTIONAL_SKIPPED,
	//! Cannot be rendered. Sending the filter set without it would return extra rows.
	UNSUPPORTED,
};

//! How the reader must rebuild a value that crossed as text, or nullptr when it needs no rebuilding.
//! Only the types ValueNode stringifies can need one; VARCHAR and ENUM are excluded because the text
//! IS the value. Anything not reconstructible from its text (BLOB, indistinguishable from VARCHAR)
//! is tagged "opaque" so the reader refuses it by name rather than comparing the wrong thing.
inline const char *ValueTypeTag(const Value &val) {
	if (val.IsNull()) {
		return nullptr;
	}
	switch (val.type().id()) {
	// Sent as JSON scalars, so they arrive already typed.
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	// Text that is itself the value.
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
		return nullptr;
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return "time";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamp";
	case LogicalTypeId::DECIMAL:
		return "decimal";
	// Outside JSON's safe integer range, so ValueNode sends the digits as text.
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "int";
	default:
		return "opaque";
	}
}

struct FilterSerializeResult {
	//! False if any filter hit UNSUPPORTED. The caller must not send a partial filter set.
	bool all_exact = true;
	string first_unsupported;
};

// Non-primitive types fall back to their DuckDB string representation.
inline yyjson_mut_val *ValueNode(yyjson_mut_doc *doc, const Value &val) {
	if (val.IsNull()) {
		return yyjson_mut_null(doc);
	}
	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(doc, val.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return yyjson_mut_sint(doc, val.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, val.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(doc, val.GetValue<double>());
	default: {
		auto s = val.ToString();
		return yyjson_mut_strncpy(doc, s.c_str(), s.size());
	}
	}
}

// Empty result means the comparison has no wire representation; the caller reports UNSUPPORTED.
inline string ComparisonOp(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	default:
		return "";
	}
}

// strncpy, not the non-cpy variants: those store only the pointer, which would dangle.
//
// `tag` becomes a 4th element when present, so arity is what tells the reader a value needs
// rebuilding; without it the clause stays the plain 3-element tuple every reader understands.
inline yyjson_mut_val *MakeClause(yyjson_mut_doc *doc, const string &col, const string &op, yyjson_mut_val *value,
                                  const char *tag = nullptr) {
	auto *clause = yyjson_mut_arr(doc);
	yyjson_mut_arr_add_strncpy(doc, clause, col.c_str(), col.size());
	yyjson_mut_arr_add_strncpy(doc, clause, op.c_str(), op.size());
	yyjson_mut_arr_append(clause, value);
	if (tag) {
		yyjson_mut_arr_add_strncpy(doc, clause, tag, strlen(tag));
	}
	return clause;
}

//! Render one filter into zero or more clause nodes. Nodes are appended to `out` only when the
//! result is not UNSUPPORTED, so a caller that bails leaves `out` untouched. Discarded nodes stay
//! in the document arena and are freed with it.
inline FilterFidelity TryBuildClauses(const TableFilter &filter, const string &col_name, yyjson_mut_doc *doc,
                                      vector<yyjson_mut_val *> &out) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cf = filter.Cast<ConstantFilter>();
		auto op = ComparisonOp(cf.comparison_type);
		if (op.empty()) {
			return FilterFidelity::UNSUPPORTED;
		}
		out.push_back(MakeClause(doc, col_name, op, ValueNode(doc, cf.constant), ValueTypeTag(cf.constant)));
		return FilterFidelity::EXACT;
	}
	case TableFilterType::IN_FILTER: {
		auto &inf = filter.Cast<InFilter>();
		auto *arr = yyjson_mut_arr(doc);
		// One tag for the list: every element is compared to the same column, so they share a type.
		// Taken from the first non-NULL, since a NULL carries none.
		const char *tag = nullptr;
		for (idx_t i = 0; i < inf.values.size(); i++) {
			yyjson_mut_arr_append(arr, ValueNode(doc, inf.values[i]));
			if (!tag) {
				tag = ValueTypeTag(inf.values[i]);
			}
		}
		out.push_back(MakeClause(doc, col_name, "in", arr, tag));
		return FilterFidelity::EXACT;
	}
	case TableFilterType::IS_NULL:
		out.push_back(MakeClause(doc, col_name, "is_null", yyjson_mut_null(doc)));
		return FilterFidelity::EXACT;
	case TableFilterType::IS_NOT_NULL:
		out.push_back(MakeClause(doc, col_name, "is_not_null", yyjson_mut_null(doc)));
		return FilterFidelity::EXACT;
	case TableFilterType::OPTIONAL_FILTER: {
		// Correctness does not depend on this filter, so an unrenderable child is safe to drop.
		auto &opt = filter.Cast<OptionalFilter>();
		if (!opt.child_filter) {
			return FilterFidelity::OPTIONAL_SKIPPED;
		}
		vector<yyjson_mut_val *> child_out;
		auto fidelity = TryBuildClauses(*opt.child_filter, col_name, doc, child_out);
		if (fidelity == FilterFidelity::UNSUPPORTED) {
			return FilterFidelity::OPTIONAL_SKIPPED;
		}
		for (auto *clause : child_out) {
			out.push_back(clause);
		}
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
		vector<yyjson_mut_val *> collected;
		for (auto &child : conj.child_filters) {
			vector<yyjson_mut_val *> child_out;
			if (TryBuildClauses(*child, col_name, doc, child_out) == FilterFidelity::UNSUPPORTED) {
				return FilterFidelity::UNSUPPORTED;
			}
			for (auto *clause : child_out) {
				collected.push_back(clause);
			}
		}
		for (auto *clause : collected) {
			out.push_back(clause);
		}
		return collected.empty() ? FilterFidelity::OPTIONAL_SKIPPED : FilterFidelity::EXACT;
	}
	default:
		return FilterFidelity::UNSUPPORTED;
	}
}

inline void SerializeFiltersInto(const TableFilterSet &filter_set, const unordered_map<idx_t, idx_t> &filter_to_col,
                                 const vector<string> &column_names, yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                 FilterSerializeResult &result) {
	auto mark_unsupported = [&result](const string &col) {
		if (result.all_exact) {
			result.first_unsupported = col;
		}
		result.all_exact = false;
	};

	for (auto &entry : filter_set.filters) {
		auto it = filter_to_col.find(entry.first);
		if (it == filter_to_col.end() || it->second >= column_names.size()) {
			// No column mapping (a row-id filter, say). It cannot be named on the wire, and
			// dropping it silently would return extra rows.
			mark_unsupported("?");
			continue;
		}
		auto &col_name = column_names[it->second];
		vector<yyjson_mut_val *> clauses;
		if (TryBuildClauses(*entry.second, col_name, doc, clauses) == FilterFidelity::UNSUPPORTED) {
			mark_unsupported(col_name);
			continue;
		}
		for (auto *clause : clauses) {
			yyjson_mut_arr_append(arr, clause);
		}
	}
}

// Standalone JSON string form; returns "" if no clauses survive (caller treats as "no filters").
inline string SerializeFilters(const TableFilterSet &filter_set, const unordered_map<idx_t, idx_t> &filter_to_col,
                               const vector<string> &column_names, FilterSerializeResult &result) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	SerializeFiltersInto(filter_set, filter_to_col, column_names, doc, root, result);

	string out;
	if (yyjson_mut_arr_size(root) > 0) {
		size_t len = 0;
		auto *json = yyjson_mut_write(doc, 0, &len);
		out.assign(json, len);
		free(json);
	}
	yyjson_mut_doc_free(doc);
	return out;
}

[[noreturn]] inline void ThrowUnrenderableFilter(const string &context, const string &column) {
	throw NotImplementedException(
	    "%s: a filter pushed down on column '%s' has no wire representation. Sending the remaining "
	    "filters would return extra rows, so the scan is refused rather than answered incorrectly.",
	    context, column);
}

} // namespace filter_json

} // namespace duckdb
