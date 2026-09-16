#include "vcat_permissions.hpp"

#include "vcat_schema_entry_base.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

// A PK is a UNIQUE+IsPrimaryKey constraint; single-column is stored as an index, multi-column by name.
static vector<string> NativeTablePrimaryKey(TableCatalogEntry &table) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		if (unique.HasIndex()) {
			return {table.GetColumn(unique.GetIndex()).GetName()};
		}
		return unique.GetColumnNames();
	}
	return {};
}

void CollectNativeSchemaPermissions(ClientContext &context, SchemaCatalogEntry &schema, const string &schema_name,
                                    optional_ptr<const string> table_filter, case_insensitive_set_t &seen,
                                    vector<TablePermissionRow> &out) {
	const auto matches = [&](const string &n) {
		return !table_filter || StringUtil::CIEquals(n, *table_filter);
	};
	// Tables and views share one catalog set, so classify on actual type and dedup via `seen`.
	auto scan_type = [&](CatalogType type) {
		schema.Scan(context, type, [&](CatalogEntry &e) {
			if (!matches(e.name) || seen.count(e.name)) {
				return;
			}
			if (e.type == CatalogType::VIEW_ENTRY) {
				seen.insert(e.name);
				out.push_back({schema_name, e.name, "native_view", {}, {"select"}});
			} else if (e.type == CatalogType::TABLE_ENTRY) {
				seen.insert(e.name);
				out.push_back({schema_name,
				               e.name,
				               "native_table",
				               NativeTablePrimaryKey(e.Cast<TableCatalogEntry>()),
				               {"select", "insert", "update", "delete", "alter"}});
			}
		});
	};
	scan_type(CatalogType::TABLE_ENTRY);
	scan_type(CatalogType::VIEW_ENTRY);
}

static void CollectCatalogPermissionsHonoringReadOnlyAttach(ClientContext &context, Catalog &catalog,
                                                            optional_ptr<const string> schema_filter,
                                                            optional_ptr<const string> table_filter,
                                                            vector<TablePermissionRow> &out) {
	// Two-pass: gather schema refs under the catalog's schema lock, then scan each after it releases.
	vector<reference<SchemaCatalogEntry>> schemas;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		schemas.push_back(s);
	});
	const idx_t first = out.size();
	for (auto &sref : schemas) {
		auto &s = sref.get();
		case_insensitive_set_t seen;
		CollectNativeSchemaPermissions(context, s, s.name, table_filter, seen, out);
	}
	// A READ_ONLY attach makes every table unwritable whatever its own type says. Narrowed here so a
	// client sees it in the reported verbs rather than only when its write is rejected.
	if (catalog.GetAttached().IsReadOnly()) {
		for (idx_t i = first; i < out.size(); i++) {
			out[i].verbs = out[i].verbs.empty() ? out[i].verbs : vector<string> {"select"};
		}
	}
}

PermissionsArgs ParsePermissionsArgs(TableFunctionBindInput &input) {
	PermissionsArgs args;
	args.catalog = input.inputs[0].GetValue<string>();
	auto sit = input.named_parameters.find("schema");
	if (sit != input.named_parameters.end() && !sit->second.IsNull()) {
		args.schema = sit->second.GetValue<string>();
		args.has_schema = true;
	}
	auto tit = input.named_parameters.find("table");
	if (tit != input.named_parameters.end() && !tit->second.IsNull()) {
		args.table = tit->second.GetValue<string>();
		args.has_table = true;
	}
	return args;
}

static void SetTablePermissionsReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"schema", "name", "kind", "primary_key", "verbs"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)};
}

namespace {
struct TablePermissionsBindData : public TableFunctionData {
	vector<TablePermissionRow> rows;
};

struct TablePermissionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};
} // namespace

static unique_ptr<GlobalTableFunctionState> TablePermissionsInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<TablePermissionsState>();
}

static void TablePermissionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TablePermissionsBindData>();
	auto &state = data_p.global_state->Cast<TablePermissionsState>();
	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value(row.schema));
		output.SetValue(1, count, Value(row.name));
		output.SetValue(2, count, Value(row.kind));
		vector<Value> pk_vals;
		for (auto &col : row.primary_key) {
			pk_vals.push_back(Value(col));
		}
		output.SetValue(3, count, Value::LIST(LogicalType::VARCHAR, std::move(pk_vals)));
		vector<Value> verb_vals;
		for (auto &verb : row.verbs) {
			verb_vals.push_back(Value(verb));
		}
		output.SetValue(4, count, Value::LIST(LogicalType::VARCHAR, std::move(verb_vals)));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

// True when every schema this catalog hands out is one of *this binary's* wrappers. Asked per
// catalog rather than by comparing GetCatalogType() to a fixed string, so the same code serves the
// bridge and the provider without either naming the other.
static bool TryCollectPermissionsThroughSchemaWrappers(ClientContext &context, Catalog &catalog,
                                                       optional_ptr<const string> schema_filter,
                                                       optional_ptr<const string> table_filter,
                                                       vector<TablePermissionRow> &out) {
	vector<reference<VirtualCatalogSchemaEntryBase>> wrappers;
	bool all_wrapped = true;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		auto *wrapper = dynamic_cast<VirtualCatalogSchemaEntryBase *>(&s);
		if (!wrapper) {
			all_wrapped = false;
			return;
		}
		wrappers.push_back(*wrapper);
	});
	if (!all_wrapped) {
		return false;
	}
	// Collected after ScanSchemas returns: a wrapper's fill can query a source or call a UDF, and the
	// catalog's schema lock must not be held across that.
	for (auto &wrapper : wrappers) {
		wrapper.get().CollectPermissions(context, table_filter, out);
	}
	return true;
}

void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	if (TryCollectPermissionsThroughSchemaWrappers(context, catalog, schema_filter, table_filter, out)) {
		return;
	}
	CollectCatalogPermissionsHonoringReadOnlyAttach(context, catalog, schema_filter, table_filter, out);
}

static unique_ptr<FunctionData> TablePermissionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	SetTablePermissionsReturnSchema(return_types, names);

	auto args = ParsePermissionsArgs(input);
	auto &catalog = Catalog::GetCatalog(context, args.catalog);
	auto result = make_uniq<TablePermissionsBindData>();
	optional_ptr<const string> table_arg = args.has_table ? optional_ptr<const string>(&args.table) : nullptr;
	optional_ptr<const string> schema_arg = args.has_schema ? optional_ptr<const string>(&args.schema) : nullptr;
	CollectCatalogPermissions(context, catalog, schema_arg, table_arg, result->rows);
	return std::move(result);
}

void RegisterTablePermissionsFunction(ExtensionLoader &loader, const string &function_name) {
	TableFunction func(function_name, {LogicalType::VARCHAR}, TablePermissionsScan, TablePermissionsBind,
	                   TablePermissionsInitGlobal);
	func.named_parameters["schema"] = LogicalType::VARCHAR;
	func.named_parameters["table"] = LogicalType::VARCHAR;
	loader.RegisterFunction(std::move(func));
}

} // namespace duckdb
