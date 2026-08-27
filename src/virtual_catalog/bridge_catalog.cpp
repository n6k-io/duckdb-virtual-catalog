#include "bridge_catalog.hpp"
#include "bridge_table_entry.hpp"
#include "bridge_insert.hpp"
#include "bridge_delete.hpp"
#include "bridge_update.hpp"
#include "bridge_table_function.hpp"
#include "bridge_scan_shared.hpp"
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

BridgeCatalog::BridgeCatalog(AttachedDatabase &db, shared_ptr<BridgeInfo> bridge_info_p)
    : DuckCatalog(db), bridge_info(std::move(bridge_info_p)) {
}

BridgeCatalog::~BridgeCatalog() {
}

void BridgeCatalog::OnDetach(ClientContext &context) {
}

void BridgeCatalog::Initialize(bool load_builtin) {
}

string BridgeCatalog::GetCatalogType() {
	return "virtual_catalog";
}

optional_ptr<CatalogEntry> BridgeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("Creating schemas in bridges is not supported");
}

optional_ptr<SchemaCatalogEntry> BridgeCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &schema_lookup,
                                                             OnEntryNotFound if_not_found) {
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' not found in bridge phantom catalog", schema_lookup.GetEntryName());
}

void BridgeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
}

void BridgeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("Dropping schemas in bridges is not supported");
}

PhysicalOperator &BridgeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	throw BinderException("CREATE TABLE AS is not supported in bridges");
}

PhysicalOperator &BridgeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                            optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for INSERT into bridge tables");
	}

	auto &table = op.table.Cast<BridgeTableCatalogEntry>();
	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasWritePermission(table.source_table_name)) {
			throw PermissionException("Table '%s' is read-only in this bridge", table.name);
		}
	}

	if (!op.column_index_map.empty() && plan) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}

	auto &insert =
	    planner.Make<BridgeInsert>(table, vector<LogicalType> {LogicalType::BIGINT}, op.estimated_cardinality);
	if (plan) {
		insert.children.push_back(*plan);
	}
	return insert;
}

PhysicalOperator &BridgeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                            PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for DELETE from bridge tables");
	}

	auto &table = op.table.Cast<BridgeTableCatalogEntry>();
	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasWritePermission(table.source_table_name)) {
			throw PermissionException("Table '%s' is read-only in this bridge", table.name);
		}
	}

	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto row_id_index = bound_ref.index;

	auto pk_it = bridge_info->primary_keys.find(table.source_table_name);
	if (pk_it == bridge_info->primary_keys.end()) {
		throw InternalException("virtual_catalog: no primary key registered for table '%s'", table.source_table_name);
	}
	auto pk_buffer = FindScanAndAttachPKBuffer(plan, pk_it->second);
	if (!pk_buffer) {
		throw InternalException("virtual_catalog: could not locate bridge scan for PK buffer injection");
	}

	auto &del_op = planner.Make<BridgeDelete>(table, row_id_index, vector<LogicalType> {LogicalType::BIGINT},
	                                          op.estimated_cardinality);
	del_op.Cast<BridgeDelete>().pk_buffer = pk_buffer;
	del_op.children.push_back(plan);
	return del_op;
}

PhysicalOperator &BridgeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                            PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause is not supported for UPDATE on bridge tables");
	}

	auto &table = op.table.Cast<BridgeTableCatalogEntry>();
	{
		lock_guard<mutex> lock(bridge_info->mtx);
		if (!bridge_info->HasWritePermission(table.source_table_name)) {
			throw PermissionException("Table '%s' is read-only in this bridge", table.name);
		}
	}

	auto pk_it2 = bridge_info->primary_keys.find(table.source_table_name);
	if (pk_it2 == bridge_info->primary_keys.end()) {
		throw InternalException("virtual_catalog: no primary key registered for table '%s'", table.source_table_name);
	}
	auto pk_buffer = FindScanAndAttachPKBuffer(plan, pk_it2->second);
	if (!pk_buffer) {
		throw InternalException("virtual_catalog: could not locate bridge scan for PK buffer injection");
	}

	auto &upd_op = planner.Make<BridgeUpdate>(table, op.columns, std::move(op.expressions),
	                                          vector<LogicalType> {LogicalType::BIGINT}, op.estimated_cardinality);
	upd_op.Cast<BridgeUpdate>().pk_buffer = pk_buffer;
	upd_op.children.push_back(plan);
	return upd_op;
}

DatabaseSize BridgeCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool BridgeCatalog::InMemory() {
	return true;
}

string BridgeCatalog::GetDBPath() {
	return bridge_info->bridge_id;
}

} // namespace duckdb
