#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;

struct TablePermissionRow {
	string schema;
	string name;
	string kind; // native_table | native_view | bridge_read | bridge_readwrite | provider
	bool writeable;
	bool editable;
	vector<string> primary_key;
};

// Native classification: base table is writeable+editable, view is neither. Dedups via `seen`.
void CollectNativeSchemaPermissions(ClientContext &context, SchemaCatalogEntry &schema, const string &schema_name,
                                    optional_ptr<const string> table_filter, case_insensitive_set_t &seen,
                                    vector<TablePermissionRow> &out);

struct PermissionsArgs {
	string catalog;
	bool has_schema = false;
	string schema;
	bool has_table = false;
	string table;
};

PermissionsArgs ParsePermissionsArgs(TableFunctionBindInput &input);

// virtual_catalog catalogs are collected from their schema wrappers, everything else via the
// plain-catalog fallback.
void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out);

void RegisterTablePermissionsFunction(ExtensionLoader &loader);

} // namespace duckdb
