#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "crossing.hpp"

namespace duckdb {

class CrossingTableCatalogEntry : public TableCatalogEntry {
public:
	CrossingTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                          CrossingSource &source, string source_schema, CrossingTable described);

	CrossingSource &Source() {
		return source;
	}

	CrossingSource &source;
	string source_schema;
	CrossingTable described;

	//! The declared key, as column positions in Describe order.
	vector<column_t> KeyColumnIndexes() const;

	//! The key column a virtual id stands for, or invalid for any other id.
	optional_idx KeyColumnOfAlias(column_t virtual_id) const;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	//! An update, a delete or a merge addresses rows by the key. The binder only takes row id
	//! columns from the virtual map, so each key column has a virtual alias there; the pass turns
	//! the aliases back into the real columns before anything crosses.
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
};

//! Turns every key alias in a crossing scan's column ids into the key column it stands for.
void ResolveKeyAliases(LogicalOperator &plan);

} // namespace duckdb
