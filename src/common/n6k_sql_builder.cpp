#include "n6k_sql_builder.hpp"

#include "ws_json.hpp"
#include "n6k_sql_escape.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>

namespace duckdb {
namespace n6k {

std::string QuoteQualifiedTable(const std::string &catalog, const std::string &schema, const std::string &table) {
	return QuoteIdent(catalog) + "." + QuoteIdent(schema) + "." + QuoteIdent(table);
}

std::string JsonValueToSqlLiteral(duckdb_yyjson::yyjson_val *v) {
	using namespace duckdb_yyjson; // NOLINT
	if (!v || yyjson_is_null(v)) {
		return "NULL";
	}
	if (yyjson_is_bool(v)) {
		return yyjson_get_bool(v) ? "TRUE" : "FALSE";
	}
	if (yyjson_is_str(v)) {
		return QuoteSqlLiteral(yyjson_get_str(v));
	}
	if (yyjson_is_num(v)) {
		size_t len = 0;
		char *txt = yyjson_val_write(v, 0, &len);
		std::string out = txt ? std::string(txt, len) : std::string("NULL");
		if (txt) {
			free(txt);
		}
		return out;
	}
	throw InvalidInputException("n6k: unsupported filter value type");
}

std::string JsonFilterToSqlClause(duckdb_yyjson::yyjson_val *clause) {
	using namespace duckdb_yyjson; // NOLINT
	// Arity is the discriminator: 2 is a group, 3 is a flat comparison. Anything else is rejected
	// rather than truncated -- a 4-element clause means the sender and this parser disagree about
	// the shape, and silently dropping the tail would apply a filter nobody asked for.
	if (!yyjson_is_arr(clause) || yyjson_arr_size(clause) < 2 || yyjson_arr_size(clause) > 3) {
		throw InvalidInputException("n6k: malformed filter clause");
	}
	auto *head = yyjson_arr_get(clause, 0);
	if (!yyjson_is_str(head)) {
		throw InvalidInputException("n6k: filter column/group must be a string");
	}

	if (yyjson_arr_size(clause) == 2) {
		std::string kind = yyjson_get_str(head);
		if (kind != "and" && kind != "or") {
			throw InvalidInputException("n6k: unsupported filter group '%s'", kind);
		}
		auto *children = yyjson_arr_get(clause, 1);
		if (!children || !yyjson_is_arr(children) || yyjson_arr_size(children) == 0) {
			throw InvalidInputException("n6k: malformed '%s' filter group", kind);
		}
		const std::string joiner = (kind == "and") ? " AND " : " OR ";
		std::string group;
		size_t j, jmax;
		yyjson_val *child;
		yyjson_arr_foreach(children, j, jmax, child) {
			if (!group.empty()) {
				group += joiner;
			}
			group += JsonFilterToSqlClause(child);
		}
		return "(" + group + ")";
	}

	auto *op_v = yyjson_arr_get(clause, 1);
	if (!yyjson_is_str(op_v)) {
		throw InvalidInputException("n6k: filter operator must be a string");
	}
	std::string col = QuoteIdent(yyjson_get_str(head));
	std::string op = yyjson_get_str(op_v);
	if (op == "is_null") {
		return col + " IS NULL";
	}
	if (op == "is_not_null") {
		return col + " IS NOT NULL";
	}
	if (op == "in") {
		auto *val_v = yyjson_arr_get(clause, 2);
		if (!val_v || !yyjson_is_arr(val_v)) {
			throw InvalidInputException("n6k: 'in' filter requires a list value");
		}
		std::string vals;
		size_t j, jmax;
		yyjson_val *item;
		bool first = true;
		yyjson_arr_foreach(val_v, j, jmax, item) {
			if (!first) {
				vals += ", ";
			}
			vals += JsonValueToSqlLiteral(item);
			first = false;
		}
		return col + " IN (" + vals + ")";
	}
	if (op == "=" || op == "!=" || op == "<" || op == "<=" || op == ">" || op == ">=") {
		return col + " " + op + " " + JsonValueToSqlLiteral(yyjson_arr_get(clause, 2));
	}
	throw InvalidInputException("n6k: unsupported filter operator '%s'", op);
}

std::string BuildPredicate(duckdb_yyjson::yyjson_val *filters) {
	using namespace duckdb_yyjson; // NOLINT
	if (!filters || !yyjson_is_arr(filters)) {
		return "";
	}
	std::string out;
	size_t idx, max;
	yyjson_val *clause;
	yyjson_arr_foreach(filters, idx, max, clause) {
		if (!out.empty()) {
			out += " AND ";
		}
		out += JsonFilterToSqlClause(clause);
	}
	return out;
}

std::string BuildScanSql(const std::string &catalog, const std::string &schema, const std::string &table,
                         const std::vector<std::string> &columns, duckdb_yyjson::yyjson_val *filters) {
	std::string col_list = "*";
	if (!columns.empty()) {
		col_list.clear();
		for (size_t i = 0; i < columns.size(); i++) {
			if (i) {
				col_list += ", ";
			}
			col_list += QuoteIdent(columns[i]);
		}
	}
	std::string sql = "SELECT " + col_list + " FROM " + QuoteQualifiedTable(catalog, schema, table);
	std::string where = BuildPredicate(filters);
	if (!where.empty()) {
		sql += " WHERE " + where;
	}
	return sql;
}

std::string BuildTableSchemaSql(const std::string &catalog, const std::string &schema, const std::string &table) {
	return "SELECT * FROM " + QuoteQualifiedTable(catalog, schema, table) + " LIMIT 0";
}

static bool IsSupportedAggregateFn(const std::string &fn) {
	return fn == "count" || fn == "sum" || fn == "min" || fn == "max" || fn == "avg";
}

std::string BuildAggregateSql(const std::string &catalog, const std::string &schema, const std::string &table,
                              duckdb_yyjson::yyjson_val *filters, const std::vector<std::string> &group_by,
                              duckdb_yyjson::yyjson_val *aggregates) {
	using namespace duckdb_yyjson; // NOLINT
	std::string select_parts;
	auto append_part = [&select_parts](const std::string &part) {
		if (!select_parts.empty()) {
			select_parts += ", ";
		}
		select_parts += part;
	};

	for (size_t i = 0; i < group_by.size(); i++) {
		append_part(QuoteIdent(group_by[i]) + " AS " + QuoteIdent("g" + std::to_string(i)));
	}

	if (aggregates && yyjson_is_arr(aggregates)) {
		size_t idx, max;
		yyjson_val *agg;
		size_t j = 0;
		yyjson_arr_foreach(aggregates, idx, max, agg) {
			if (!yyjson_is_obj(agg)) {
				throw InvalidInputException("n6k: malformed aggregate");
			}
			auto fn = JsonGetStr(agg, "fn");
			if (!IsSupportedAggregateFn(fn)) {
				throw InvalidInputException("n6k: unsupported aggregate function '%s'", fn.empty() ? "?" : fn);
			}
			auto *col_v = yyjson_obj_get(agg, "col");
			std::string expr;
			if (!col_v || yyjson_is_null(col_v)) {
				if (fn != "count") {
					throw InvalidInputException("n6k: aggregate '%s' requires a column", fn);
				}
				expr = "count(*)";
			} else {
				if (!yyjson_is_str(col_v)) {
					throw InvalidInputException("n6k: aggregate column must be a string");
				}
				expr = fn + "(" + QuoteIdent(yyjson_get_str(col_v)) + ")";
			}
			append_part(expr + " AS " + QuoteIdent("a" + std::to_string(j)));
			j++;
		}
	}

	if (select_parts.empty()) {
		throw InvalidInputException("n6k: aggregate request has no group keys and no aggregates");
	}

	std::string sql = "SELECT " + select_parts + " FROM " + QuoteQualifiedTable(catalog, schema, table);
	std::string where = BuildPredicate(filters);
	if (!where.empty()) {
		sql += " WHERE " + where;
	}
	if (!group_by.empty()) {
		sql += " GROUP BY ";
		for (size_t i = 0; i < group_by.size(); i++) {
			if (i) {
				sql += ", ";
			}
			sql += std::to_string(i + 1);
		}
	}
	return sql;
}

std::string BuildCreateTableSql(const std::string &catalog, const std::string &schema, const std::string &name,
                                duckdb_yyjson::yyjson_val *columns) {
	using namespace duckdb_yyjson; // NOLINT
	if (!columns || !yyjson_is_arr(columns)) {
		throw InvalidInputException("n6k: create_table requires a \"columns\" array");
	}
	std::string col_defs;
	size_t idx, max;
	yyjson_val *col;
	yyjson_arr_foreach(columns, idx, max, col) {
		auto cname = JsonGetStr(col, "name");
		auto ctype = JsonGetStr(col, "type");
		if (cname.empty() || ctype.empty()) {
			throw InvalidInputException("n6k: each column requires \"name\" and \"type\"");
		}
		if (!col_defs.empty()) {
			col_defs += ", ";
		}
		col_defs += QuoteIdent(cname) + " " + ctype;
	}
	return "CREATE TABLE " + QuoteQualifiedTable(catalog, schema, name) + " (" + col_defs + ")";
}

std::string BuildAlterSql(const std::string &catalog, const std::string &schema, const std::string &table,
                          const std::string &kind, duckdb_yyjson::yyjson_val *details) {
	std::string qualified = QuoteQualifiedTable(catalog, schema, table);
	if (kind == "add_column") {
		auto cname = JsonGetStr(details, "name");
		auto ctype = JsonGetStr(details, "type");
		if (cname.empty() || ctype.empty()) {
			throw InvalidInputException("n6k: add_column requires \"name\" and \"type\"");
		}
		return "ALTER TABLE " + qualified + " ADD COLUMN " + QuoteIdent(cname) + " " + ctype;
	}
	if (kind == "drop_column") {
		auto cname = JsonGetStr(details, "name");
		if (cname.empty()) {
			throw InvalidInputException("n6k: drop_column requires \"name\"");
		}
		return "ALTER TABLE " + qualified + " DROP COLUMN " + QuoteIdent(cname);
	}
	if (kind == "rename_column") {
		auto old_name = JsonGetStr(details, "old_name");
		auto new_name = JsonGetStr(details, "new_name");
		if (old_name.empty() || new_name.empty()) {
			throw InvalidInputException("n6k: rename_column requires \"old_name\" and \"new_name\"");
		}
		return "ALTER TABLE " + qualified + " RENAME COLUMN " + QuoteIdent(old_name) + " TO " + QuoteIdent(new_name);
	}
	throw InvalidInputException("n6k: unsupported alter kind '%s' (add_column, drop_column, rename_column)", kind);
}

std::string RenderRpcArgValue(duckdb_yyjson::yyjson_val *v) {
	using namespace duckdb_yyjson; // NOLINT
	if (v && yyjson_is_obj(v)) {
		std::string out = "{";
		size_t idx, max;
		yyjson_val *key;
		yyjson_val *val;
		yyjson_obj_foreach(v, idx, max, key, val) {
			if (idx > 0) {
				out += ", ";
			}
			const char *kstr = yyjson_get_str(key);
			out += QuoteSqlLiteral(kstr ? std::string(kstr) : std::string()) + ": " + RenderRpcArgValue(val);
		}
		out += "}";
		return out;
	}
	if (v && yyjson_is_arr(v)) {
		std::string out = "[";
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(v, idx, max, item) {
			if (idx > 0) {
				out += ", ";
			}
			out += RenderRpcArgValue(item);
		}
		out += "]";
		return out;
	}
	return JsonValueToSqlLiteral(v);
}

std::string BuildRpcCallSql(const std::string &catalog, const std::string &schema, const std::string &function,
                            duckdb_yyjson::yyjson_val *args, const std::string &table_arg) {
	using namespace duckdb_yyjson; // NOLINT
	if (function.empty()) {
		throw InvalidInputException("n6k: rpc requires a function name");
	}

	std::string target = QuoteIdent(function);
	if (!catalog.empty()) {
		target = QuoteIdent(catalog) + "." + QuoteIdent(schema) + "." + target;
	}

	std::string rendered;
	if (!table_arg.empty()) {
		rendered = table_arg;
	}
	if (args && yyjson_is_arr(args)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(args, idx, max, item) {
			if (!rendered.empty()) {
				rendered += ", ";
			}
			rendered += RenderRpcArgValue(item);
		}
	}
	return "SELECT * FROM " + target + "(" + rendered + ")";
}

} // namespace n6k
} // namespace duckdb
