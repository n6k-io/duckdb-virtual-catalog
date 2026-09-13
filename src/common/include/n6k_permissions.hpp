#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;
class DatabaseInstance;

struct TablePermissionRow {
	string schema;
	string name;
	string kind; // native_table | native_view | bridge_read | bridge_readwrite | provider | n6k_remote
	bool writeable;
	bool editable;
	vector<string> primary_key;
};

// Native classification: base table is writeable+editable, view is neither. Dedups via `seen`.
void CollectNativeSchemaPermissions(ClientContext &context, SchemaCatalogEntry &schema, const string &schema_name,
                                    optional_ptr<const string> table_filter, case_insensitive_set_t &seen,
                                    vector<TablePermissionRow> &out);

void CollectCatalogPermissionsHonoringReadOnlyAttach(ClientContext &context, Catalog &catalog,
                                                     optional_ptr<const string> schema_filter,
                                                     optional_ptr<const string> table_filter,
                                                     vector<TablePermissionRow> &out);

struct TablePermissionsBindData : public TableFunctionData {
	vector<TablePermissionRow> rows;
};

struct PermissionsArgs {
	string catalog;
	bool has_schema = false;
	string schema;
	bool has_table = false;
	string table;
};

PermissionsArgs ParsePermissionsArgs(TableFunctionBindInput &input);
void SetTablePermissionsReturnSchema(vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<GlobalTableFunctionState> TablePermissionsInitGlobal(ClientContext &context, TableFunctionInitInput &input);
void TablePermissionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

using PermissionCollectorFn = void (*)(ClientContext &context, Catalog &catalog,
                                       optional_ptr<const string> schema_filter,
                                       optional_ptr<const string> table_filter, vector<TablePermissionRow> &out);

// Dispatches to the registered collector for `catalog`'s type, then to the virtual_catalog extensions'
// own `<prefix>_table_permissions` for their catalog types, else the plain-catalog fallback.
void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out);

// Registers a collector for a catalog type; the shared bind dispatches through this since the extensions don't link
// each other.
void RegisterPermissionCollector(DatabaseInstance &db, const string &catalog_type, PermissionCollectorFn collector);

// Registers the shared n6k_table_permissions table function; idempotent across extensions.
void RegisterTablePermissionsFunction(ExtensionLoader &loader);

} // namespace duckdb
