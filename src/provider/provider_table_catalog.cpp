#include "provider_table_catalog.hpp"
#include "provider_scan_attach.hpp"
#include "provider_table_entry.hpp"
#include "provider_table_insert.hpp"
#include "provider_table_delete.hpp"
#include "provider_table_update.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

ProviderTableCatalog::ProviderTableCatalog(AttachedDatabase &db) : DuckCatalog(db) {
}

ProviderTableCatalog::~ProviderTableCatalog() {
}

void ProviderTableCatalog::OnDetach(ClientContext &context) {
}

void ProviderTableCatalog::Initialize(bool load_builtin) {
}

string ProviderTableCatalog::GetCatalogType() {
	return "virtual_catalog_provider_table";
}

optional_ptr<CatalogEntry> ProviderTableCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("Creating schemas in a provider-table catalog is not supported");
}

optional_ptr<SchemaCatalogEntry> ProviderTableCatalog::LookupSchema(CatalogTransaction transaction,
                                                                    const EntryLookupInfo &schema_lookup,
                                                                    OnEntryNotFound if_not_found) {
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' not found in provider-table catalog", schema_lookup.GetEntryName());
}

void ProviderTableCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
}

void ProviderTableCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("Dropping schemas in a provider-table catalog is not supported");
}

PhysicalOperator &ProviderTableCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                          LogicalCreateTable &op, PhysicalOperator &plan) {
	throw BinderException("CREATE TABLE AS is not supported for provider tables");
}

PhysicalOperator &ProviderTableCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for INSERT into provider tables");
	}

	auto &table = op.table.Cast<ProviderTableCatalogEntry>();
	if (!table.table_info->provider) {
		throw BinderException("Provider table '%s' has no provider bound", table.name);
	}
	// Empty UDF name means the host did not register a UDF for this op in provider_register.
	if (table.table_info->provider->insert_udf.empty()) {
		throw PermissionException("Provider table '%s' does not support INSERT", table.name);
	}

	if (!op.column_index_map.empty() && plan) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}

	auto &insert =
	    planner.Make<ProviderTableInsert>(table, vector<LogicalType> {LogicalType::BIGINT}, op.estimated_cardinality);
	if (plan) {
		insert.children.push_back(*plan);
	}
	return insert;
}

PhysicalOperator &ProviderTableCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalDelete &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for DELETE from provider tables");
	}

	auto &table = op.table.Cast<ProviderTableCatalogEntry>();
	if (!table.table_info->provider) {
		throw BinderException("Provider table '%s' has no provider bound", table.name);
	}
	if (table.table_info->provider->delete_udf.empty()) {
		throw PermissionException("Provider table '%s' does not support DELETE", table.name);
	}

	if (table.table_info->primary_keys.empty()) {
		throw BinderException("Provider table '%s' has no primary key declared; DELETE requires one", table.name);
	}

	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto row_id_index = bound_ref.index;

	auto pk_buffer = AttachPKBufferToTargetScans(plan, table.table_info->primary_keys, table);
	if (!pk_buffer) {
		throw InternalException("virtual_catalog_provider: could not locate a scan of '%s' for PK buffer injection",
		                        table.name);
	}

	auto &del_op = planner.Make<ProviderTableDelete>(table, row_id_index, vector<LogicalType> {LogicalType::BIGINT},
	                                                 op.estimated_cardinality);
	del_op.Cast<ProviderTableDelete>().pk_buffer = pk_buffer;
	del_op.children.push_back(plan);
	return del_op;
}

PhysicalOperator &ProviderTableCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalUpdate &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for UPDATE on provider tables");
	}

	auto &table = op.table.Cast<ProviderTableCatalogEntry>();
	if (!table.table_info->provider) {
		throw BinderException("Provider table '%s' has no provider bound", table.name);
	}
	if (table.table_info->provider->update_udf.empty()) {
		throw PermissionException("Provider table '%s' does not support UPDATE", table.name);
	}

	if (table.table_info->primary_keys.empty()) {
		throw BinderException("Provider table '%s' has no primary key declared; UPDATE requires one", table.name);
	}

	auto pk_buffer = AttachPKBufferToTargetScans(plan, table.table_info->primary_keys, table);
	if (!pk_buffer) {
		throw InternalException("virtual_catalog_provider: could not locate a scan of '%s' for PK buffer injection",
		                        table.name);
	}

	auto &upd_op =
	    planner.Make<ProviderTableUpdate>(table, op.columns, std::move(op.expressions), std::move(op.bound_defaults),
	                                      vector<LogicalType> {LogicalType::BIGINT}, op.estimated_cardinality);
	upd_op.Cast<ProviderTableUpdate>().pk_buffer = pk_buffer;
	upd_op.children.push_back(plan);
	return upd_op;
}

DatabaseSize ProviderTableCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool ProviderTableCatalog::InMemory() {
	return true;
}

string ProviderTableCatalog::GetDBPath() {
	return "provider_table";
}

} // namespace duckdb
