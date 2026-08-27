#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "provider_table_info.hpp"

namespace duckdb {

class ProviderTableCatalogEntry : public TableCatalogEntry {
public:
	ProviderTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                          shared_ptr<ProviderTableInfo> table_info);

	shared_ptr<ProviderTableInfo> table_info;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
