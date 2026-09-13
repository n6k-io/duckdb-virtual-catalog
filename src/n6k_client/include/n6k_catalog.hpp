#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_schema_entry.hpp"

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

struct N6kAttachInfo;

class N6kCatalog : public Catalog {
public:
	N6kCatalog(AttachedDatabase &db, string base_url, string token, std::shared_ptr<CatalogSession> session);
	~N6kCatalog() override;

	string base_url;
	string token;
	std::shared_ptr<CatalogSession> session;

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
	void OnDetach(ClientContext &context) override;

	N6kSchemaEntry &GetOrCreateSchema(CatalogTransaction transaction, const string &schema_name);

	// Lazily fetch the server schema list once; idempotent, thread-safe, retries on failure.
	void LoadSchemasOnce();

	// Parses {"schemas":[...]} from a PUSH frame body and invalidates those schemas.
	void HandlePushInvalidate(const std::string &body);

	void InvalidateSchemas(const std::vector<std::string> &schema_names);

private:
	mutex schemas_lock;
	case_insensitive_map_t<unique_ptr<N6kSchemaEntry>> schemas;

	// Guards one-time lazy discovery. Lock ordering: schemas_load_lock → schemas_lock, never the reverse.
	mutex schemas_load_lock;
	bool schemas_loaded_ = false;
};

} // namespace duckdb
