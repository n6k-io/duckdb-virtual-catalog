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
	string kind; // native_table | native_view | bridge | provider
	vector<string> primary_key;
	//! In select/insert/update/delete/alter order. The only capability signal there is: do not add
	//! a coarse `writeable`/`editable` boolean beside it.
	vector<string> verbs;
};

// Native classification: a base table accepts every verb, a view only select. Dedups via `seen`.
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

// A catalog whose schemas are VirtualCatalogSchemaEntryBase wrappers is collected through them, so
// the owning extension classifies its own entries; everything else takes the plain-catalog
// fallback. The extensions are separate binaries, so the *other* one's catalog fails the cast and
// is reported as a plain DuckDB catalog.
void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out);

//! `function_name` is bridge_table_permissions or provider_table_permissions -- each extension owns
//! its own name, so both can be loaded into one process.
void RegisterTablePermissionsFunction(ExtensionLoader &loader, const string &function_name);

} // namespace duckdb
