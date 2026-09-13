#include "crossing_table_entry.hpp"

#include "crossing_catalog.hpp"
#include "crossing_scan.hpp"
#include "crossing_transactions.hpp"

#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

CrossingTableCatalogEntry::CrossingTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                     CreateTableInfo &info, CrossingSource &source_p,
                                                     string source_schema_p, CrossingTable described_p)
    : TableCatalogEntry(catalog, schema, info), source(source_p), source_schema(std::move(source_schema_p)),
      described(std::move(described_p)) {
}

vector<column_t> CrossingTableCatalogEntry::KeyColumnIndexes() const {
	vector<column_t> out;
	for (auto &key : described.KeyColumns()) {
		out.push_back(columns.GetColumn(key).Logical().index);
	}
	return out;
}

namespace {

constexpr column_t KEY_ALIAS_BASE = UINT64_C(9223372036854775808) + 1;

} // namespace

optional_idx CrossingTableCatalogEntry::KeyColumnOfAlias(column_t virtual_id) const {
	if (virtual_id < KEY_ALIAS_BASE) {
		return optional_idx();
	}
	auto key = KeyColumnIndexes();
	auto k = virtual_id - KEY_ALIAS_BASE;
	if (k >= key.size()) {
		return optional_idx();
	}
	return key[k];
}

virtual_column_map_t CrossingTableCatalogEntry::GetVirtualColumns() const {
	auto out = TableCatalogEntry::GetVirtualColumns();
	auto key = KeyColumnIndexes();
	for (idx_t k = 0; k < key.size(); k++) {
		auto &column = columns.GetColumn(LogicalIndex(key[k]));
		out.insert(make_pair(KEY_ALIAS_BASE + k, TableColumn(column.Name(), column.Type())));
	}
	return out;
}

vector<column_t> CrossingTableCatalogEntry::GetRowIdColumns() const {
	vector<column_t> out;
	for (idx_t k = 0; k < described.KeyColumns().size(); k++) {
		out.push_back(KEY_ALIAS_BASE + k);
	}
	return out;
}

void ResolveKeyAliases(LogicalOperator &plan) {
	for (auto &child : plan.children) {
		ResolveKeyAliases(*child);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan.Cast<LogicalGet>();
	auto *data = dynamic_cast<CrossingScanBindData *>(get.bind_data.get());
	if (!data || !data->table) {
		return;
	}
	for (auto &column_index : get.GetMutableColumnIds()) {
		if (!column_index.IsVirtualColumn()) {
			continue;
		}
		auto key = data->table->KeyColumnOfAlias(column_index.GetPrimaryIndex());
		if (key.IsValid()) {
			column_index = ColumnIndex(key.GetIndex());
		} else if (data->fragment) {
			data->fragment->frozen = true;
		}
	}
}

unique_ptr<BaseStatistics> CrossingTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction CrossingTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	if (!described.Allows(CrossingVerb::SELECT)) {
		throw PermissionException("virtual_catalog_bridge: '%s' does not have 'select' permission", name);
	}
	bind_data = MakeCrossingScanBindData(*this, source);
	return CrossingScanFunction();
}

TableStorageInfo CrossingTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	// Only the unique constraints copied from the source's catalog, which the source enforces. A key
	// declared through bridge_primary_key is how the bridge addresses rows, not a uniqueness anyone
	// keeps, so ON CONFLICT must not be allowed to match on it.
	for (auto &constraint : GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		IndexInfo index;
		index.is_unique = true;
		index.is_primary = unique.IsPrimaryKey();
		index.is_foreign = false;
		if (unique.HasIndex()) {
			index.column_set.insert(columns.GetColumn(unique.GetIndex()).Physical().index);
		} else {
			for (auto &column_name : unique.GetColumnNames()) {
				index.column_set.insert(columns.GetColumn(column_name).Physical().index);
			}
		}
		info.index_info.push_back(std::move(index));
	}
	return info;
}

} // namespace duckdb
