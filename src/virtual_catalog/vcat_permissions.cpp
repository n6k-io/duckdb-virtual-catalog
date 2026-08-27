#include "vcat_permissions.hpp"

#include "vcat_schema_entry.hpp"

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
				out.push_back({schema_name, e.name, "native_view", false, false});
			} else if (e.type == CatalogType::TABLE_ENTRY) {
				seen.insert(e.name);
				out.push_back({schema_name, e.name, "native_table", true, true,
				               NativeTablePrimaryKey(e.Cast<TableCatalogEntry>())});
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
	// A READ_ONLY attach makes every table in the catalog unwritable no matter what its own type
	// says, so clear the bits the per-schema pass set. Reporting a table writeable here would have
	// the client discover otherwise only when its write is rejected -- and ATTACH ... (READ_ONLY) is
	// the documented way for a host to expose a read-only surface.
	if (catalog.GetAttached().IsReadOnly()) {
		for (idx_t i = first; i < out.size(); i++) {
			out[i].writeable = false;
			out[i].editable = false;
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
	names = {"schema", "name", "kind", "writeable", "editable", "primary_key"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::LIST(LogicalType::VARCHAR)};
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
		output.SetValue(3, count, Value::BOOLEAN(row.writeable));
		output.SetValue(4, count, Value::BOOLEAN(row.editable));
		vector<Value> pk_vals;
		for (auto &col : row.primary_key) {
			pk_vals.push_back(Value(col));
		}
		output.SetValue(5, count, Value::LIST(LogicalType::VARCHAR, std::move(pk_vals)));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

static void CollectBridgePermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                                     optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		s.Cast<VirtualCatalogSchemaEntry>().CollectPermissions(context, table_filter, out);
	});
}

void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	if (catalog.GetCatalogType() == "virtual_catalog") {
		CollectBridgePermissions(context, catalog, schema_filter, table_filter, out);
	} else {
		CollectCatalogPermissionsHonoringReadOnlyAttach(context, catalog, schema_filter, table_filter, out);
	}
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

void RegisterTablePermissionsFunction(ExtensionLoader &loader) {
	TableFunction func("vcat_table_permissions", {LogicalType::VARCHAR}, TablePermissionsScan, TablePermissionsBind,
	                   TablePermissionsInitGlobal);
	func.named_parameters["schema"] = LogicalType::VARCHAR;
	func.named_parameters["table"] = LogicalType::VARCHAR;
	loader.RegisterFunction(std::move(func));
}

} // namespace duckdb
