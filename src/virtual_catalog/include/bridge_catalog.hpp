#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "bridge_bridge.hpp"

namespace duckdb {

class BridgeCatalog : public DuckCatalog {
public:
	BridgeCatalog(AttachedDatabase &db, shared_ptr<BridgeInfo> bridge_info);
	~BridgeCatalog() override;
	void OnDetach(ClientContext &context) override;

	shared_ptr<BridgeInfo> bridge_info;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

private:
};

} // namespace duckdb
