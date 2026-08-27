#include "bridge_dml.hpp"

namespace duckdb {

BridgeDMLContext OpenAuthorizedSourceWriteContext(BridgeTableCatalogEntry &table) {
	{
		lock_guard<mutex> lock(table.bridge_info->mtx);
		if (!table.bridge_info->HasWritePermission(table.source_table_name)) {
			throw PermissionException("virtual_catalog: table '%s' is read-only in bridge '%s'",
			                          table.source_table_name, table.bridge_info->bridge_id);
		}
	}

	if (!table.bridge_info->source_db) {
		throw IOException("virtual_catalog: source database for bridge '%s' is unavailable",
		                  table.bridge_info->bridge_id);
	}

	auto conn = make_uniq<Connection>(*table.bridge_info->source_db);
	auto quoted_table = table.bridge_info->QualifiedSourceTable(table.source_table_name);

	auto pk_it = table.bridge_info->primary_keys.find(table.source_table_name);
	if (pk_it == table.bridge_info->primary_keys.end()) {
		throw InternalException("virtual_catalog: no primary key registered for table '%s'", table.source_table_name);
	}

	return BridgeDMLContext {std::move(conn), std::move(quoted_table), pk_it->second};
}

} // namespace duckdb
