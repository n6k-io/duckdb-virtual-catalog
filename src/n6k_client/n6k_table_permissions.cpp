#include "n6k_table_permissions.hpp"
#include "n6k_catalog_list.hpp"
#include "n6k_describe.hpp"
#include "n6k_exception_types.hpp"
#include "n6k_permissions.hpp"
#include "n6k_catalog.hpp"
#include "n6k_schema_entry.hpp"
#include "n6k_table_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Two-pass: gather schema refs under schemas_lock, then Scan each after — Scan re-locks and would deadlock inline.
static void CollectRemoteTablePermissionRows(ClientContext &context, N6kCatalog &catalog,
                                             optional_ptr<const string> schema_filter,
                                             optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	vector<reference<SchemaCatalogEntry>> schemas;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		schemas.push_back(s);
	});
	for (auto &sref : schemas) {
		auto &s = sref.get();
		s.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &e) {
			auto &t = e.Cast<N6kTableCatalogEntry>();
			if (table_filter && !StringUtil::CIEquals(t.name, *table_filter)) {
				return;
			}
			out.push_back({s.name, t.name, "n6k_remote", t.writable, t.editable, t.primary_key});
		});
	}
}

static void CollectN6kPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                                  optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	CollectRemoteTablePermissionRows(context, catalog.Cast<N6kCatalog>(), schema_filter, table_filter, out);
}

void RegisterN6kTablePermissions(ExtensionLoader &loader) {
	RegisterPermissionCollector(loader.GetDatabaseInstance(), "n6k", CollectN6kPermissions);
	RegisterTablePermissionsFunction(loader);
	RegisterTableDescribeFunction(loader);
	RegisterCatalogListFunction(loader);
	n6k::RegisterExceptionTypesFunction(loader);
}

} // namespace duckdb
