#include "vcat_catalog_base.hpp"

#include "vcat_schema_entry_base.hpp"

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

VirtualCatalogBase::VirtualCatalogBase(AttachedDatabase &db) : DuckCatalog(db) {
}

VirtualCatalogBase::~VirtualCatalogBase() = default;

VirtualCatalogSchemaEntryBase &VirtualCatalogBase::GetOrCreateWrapper(SchemaCatalogEntry &target_schema) {
	lock_guard<mutex> lock(wrappers_lock);
	auto it = wrappers.find(target_schema.name);
	if (it != wrappers.end()) {
		// Match on address, not name: DROP SCHEMA s; CREATE SCHEMA s; yields a fresh DuckSchemaEntry,
		// and the old wrapper's target_schema/target_catalog references now dangle.
		if (&it->second->TargetSchema() == &target_schema) {
			return *it->second;
		}
		retired_wrappers.push_back(std::move(it->second));
		wrappers.erase(it);
	}
	auto wrapper = CreateSchemaWrapper(target_schema);
	auto &ref = *wrapper;
	wrappers[target_schema.name] = std::move(wrapper);
	return ref;
}

optional_ptr<SchemaCatalogEntry> VirtualCatalogBase::LookupSchema(CatalogTransaction transaction,
                                                                  const EntryLookupInfo &schema_lookup,
                                                                  OnEntryNotFound if_not_found) {
	auto inner = DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
	if (!inner) {
		return nullptr;
	}
	return &GetOrCreateWrapper(*inner);
}

void VirtualCatalogBase::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DuckCatalog::ScanSchemas(context, [&](SchemaCatalogEntry &inner) { callback(GetOrCreateWrapper(inner)); });
}

void VirtualCatalogBase::DropSchema(ClientContext &context, DropInfo &info) {
	ThrowIfSchemaStillServed(info.name);
	// Mirrors DuckCatalog::DropSchema, which is private and so cannot be called from here. CREATE OR
	// REPLACE SCHEMA reaches the non-virtual (CatalogTransaction) overload and bypasses this refusal
	// entirely -- GetOrCreateWrapper's identity check is what keeps that path safe.
	if (!GetSchemaCatalogSet().DropEntry(GetCatalogTransaction(context), info.name, info.cascade) &&
	    info.if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException::MissingEntry(CatalogType::SCHEMA_ENTRY, info.name, string());
	}
}

} // namespace duckdb
