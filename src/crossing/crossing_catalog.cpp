#include "crossing_catalog.hpp"

#include "crossing_table_entry.hpp"
#include "crossing_write.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

CrossingCatalog::CrossingCatalog(AttachedDatabase &db, string catalog_type_p, unique_ptr<CrossingSource> source_p)
    : VirtualCatalogBase(db), catalog_type(std::move(catalog_type_p)), source(std::move(source_p)),
      phantom(make_shared_ptr<CrossingWriteCatalog>(db)) {
	if (!source) {
		return;
	}
	for (auto &schema : source->Schemas()) {
		auto &tables = served[schema];
		for (auto &table : source->Tables(schema)) {
			tables.insert(table);
		}
	}
}

CrossingCatalog::~CrossingCatalog() = default;

void CrossingCatalog::Initialize(optional_ptr<ClientContext>, bool load_builtin) {
	DuckCatalog::Initialize(load_builtin);
	if (served.empty()) {
		return;
	}
	// Not in the attach callback: the transaction manager does not exist until after it returns.
	auto transaction = CatalogTransaction::GetSystemTransaction(GetDatabase());
	for (auto &schema : served) {
		CreateSchemaInfo info;
		info.catalog = GetName();
		info.schema = schema.first;
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		CreateSchema(transaction, info);
	}
}

Catalog &CrossingCatalog::WriteCatalog() {
	return *phantom;
}

optional_ptr<CrossingSource> CrossingCatalog::FindSourceFor(const string &schema, const string &table) {
	auto schema_it = served.find(schema);
	if (schema_it == served.end() || schema_it->second.find(table) == schema_it->second.end()) {
		return nullptr;
	}
	return source.get();
}

vector<string> CrossingCatalog::TablesIn(const string &schema) {
	vector<string> out;
	auto schema_it = served.find(schema);
	if (schema_it == served.end()) {
		return out;
	}
	for (auto &table : schema_it->second) {
		out.push_back(table);
	}
	std::sort(out.begin(), out.end());
	return out;
}

void CrossingCatalog::ThrowIfSchemaStillServed(const string &schema_name) {
	auto schema_it = served.find(schema_name);
	if (schema_it != served.end() && !schema_it->second.empty()) {
		throw BinderException("virtual_catalog_bridge: schema '%s' is serving a bridge; DETACH the catalog "
		                      "instead of dropping it",
		                      schema_name);
	}
}

unique_ptr<VirtualCatalogSchemaEntryBase> CrossingCatalog::CreateSchemaWrapper(SchemaCatalogEntry &target_schema) {
	return make_uniq<CrossingSchemaEntry>(*this, target_schema);
}

CrossingSchemaEntry::CrossingSchemaEntry(Catalog &catalog, SchemaCatalogEntry &target_schema)
    : VirtualCatalogSchemaEntryBase(catalog, target_schema) {
}

CrossingCatalog &CrossingSchemaEntry::ParentCrossingCatalog() {
	return ParentCatalog().Cast<CrossingCatalog>();
}

const vector<string> &CrossingSchemaEntry::GetOrAskForTableNames() {
	cached_names = ParentCrossingCatalog().TablesIn(name);
	return cached_names;
}

CatalogEntry *CrossingSchemaEntry::GetOrDescribeEntry(const string &entry_name, CrossingSource &source) {
	auto it = cache.find(entry_name);
	if (it != cache.end()) {
		return it->second.get();
	}

	auto described = source.Describe(name, entry_name);
	auto &column_names = described.ColumnNames();
	auto &column_types = described.ColumnTypes();
	if (column_names.empty()) {
		throw InvalidInputException("virtual_catalog_bridge: the source described '%s' with no columns", entry_name);
	}

	// `entry_name`, not described.Name(): a source that renames a table under us would otherwise
	// produce an entry nothing can resolve.
	auto create_info = make_uniq<CreateTableInfo>(*this, entry_name);
	for (idx_t c = 0; c < column_names.size(); c++) {
		create_info->columns.AddColumn(ColumnDefinition(column_names[c], column_types[c]));
	}
	for (auto &constraint : described.Constraints()) {
		create_info->constraints.push_back(constraint->Copy());
	}
	auto entry = make_uniq<CrossingTableCatalogEntry>(ParentCrossingCatalog().WriteCatalog(), *this, *create_info,
	                                                  source, name, std::move(described));
	auto *raw = entry.get();
	cache[entry_name] = std::move(entry);
	return raw;
}

bool CrossingSchemaEntry::ServesTable(const string &entry_name) {
	return ParentCrossingCatalog().FindSourceFor(name, entry_name) != nullptr;
}

CatalogEntry *CrossingSchemaEntry::LookupExtensionEntry(CatalogTransaction, const string &entry_name) {
	lock_guard<mutex> lock(source_lock);
	auto source = ParentCrossingCatalog().FindSourceFor(name, entry_name);
	if (!source) {
		auto it = cache.find(entry_name);
		if (it != cache.end()) {
			retired.push_back(std::move(it->second));
			cache.erase(it);
		}
		return nullptr;
	}
	return GetOrDescribeEntry(entry_name, *source);
}

void CrossingSchemaEntry::ScanExtensionEntries(optional_ptr<ClientContext>, CatalogType type,
                                               case_insensitive_set_t &seen,
                                               const std::function<void(CatalogEntry &)> &callback) {
	// No ClientContext needed: the source answers from its own state, so both Scan overloads report
	// the same set.
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	lock_guard<mutex> lock(source_lock);
	for (auto &n : GetOrAskForTableNames()) {
		if (seen.count(n)) {
			continue;
		}
		auto source = ParentCrossingCatalog().FindSourceFor(name, n);
		if (!source) {
			continue;
		}
		auto *entry = GetOrDescribeEntry(n, *source);
		if (entry) {
			callback(*entry);
			seen.insert(n);
		}
	}
}

void CrossingSchemaEntry::ThrowIfExtensionOwnedOnDrop(const string &entry_name) {
	lock_guard<mutex> lock(source_lock);
	if (ServesTable(entry_name)) {
		throw BinderException("virtual_catalog_bridge: '%s' is bridge-managed; DROP is not supported", entry_name);
	}
}

bool CrossingSchemaEntry::TryAlterExtensionEntry(CatalogTransaction, AlterTableInfo &alter) {
	lock_guard<mutex> lock(source_lock);
	if (!ServesTable(alter.name)) {
		return false;
	}
	throw BinderException("virtual_catalog_bridge: '%s' is bridge-managed; ALTER TABLE is not supported", alter.name);
}

void CrossingSchemaEntry::CollectExtensionPermissions(ClientContext &, optional_ptr<const string> table_filter,
                                                      case_insensitive_set_t &seen, vector<TablePermissionRow> &out) {
	lock_guard<mutex> lock(source_lock);
	for (auto &n : GetOrAskForTableNames()) {
		if (seen.count(n)) {
			continue;
		}
		if (table_filter && !StringUtil::CIEquals(*table_filter, n)) {
			continue;
		}
		auto source = ParentCrossingCatalog().FindSourceFor(name, n);
		if (!source) {
			continue;
		}
		auto *entry = GetOrDescribeEntry(n, *source);
		if (!entry) {
			continue;
		}
		auto &described = entry->Cast<CrossingTableCatalogEntry>().described;
		TablePermissionRow row;
		row.schema = name;
		row.name = n;
		row.kind = "crossing";
		row.primary_key = described.KeyColumns();
		for (idx_t v = 0; v < CROSSING_VERB_COUNT; v++) {
			auto verb = static_cast<CrossingVerb>(v);
			if (described.Allows(verb)) {
				row.verbs.emplace_back(CrossingVerbName(verb));
			}
		}
		out.push_back(std::move(row));
		seen.insert(n);
	}
}

} // namespace duckdb
