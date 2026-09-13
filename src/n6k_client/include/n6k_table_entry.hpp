#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include <memory>

namespace duckdb {

class CatalogSession;

class N6kTableCatalogEntry : public TableCatalogEntry {
public:
	N6kTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, string base_url,
	                     string table_schema, string table_name, bool writable, bool editable,
	                     vector<string> primary_key, std::shared_ptr<CatalogSession> session = nullptr);

	string base_url;
	string table_schema;
	string table_name;
	bool writable;
	bool editable;
	vector<string> primary_key;
	std::shared_ptr<CatalogSession> session;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
