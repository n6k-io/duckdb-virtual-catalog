#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "bridge_bridge.hpp"

namespace duckdb {

class BridgeTableCatalogEntry : public TableCatalogEntry {
public:
	BridgeTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                        shared_ptr<BridgeInfo> bridge_info, string source_table_name);

	shared_ptr<BridgeInfo> bridge_info;
	string source_table_name;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
