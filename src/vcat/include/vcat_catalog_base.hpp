#pragma once

// The half of the old VirtualCatalog that neither extension owns: a DuckCatalog that hands out a
// wrapper per DuckSchemaEntry so extension entries can coexist with native ones in the same schema.
// What an extension entry *is* -- a bridge route or a provider table -- is the subclass's business.
//
// Lock order: wrappers_lock is a leaf and is never held while acquiring anything else. A subclass
// adding its own locks must keep them below this one.

#include "duckdb.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

class VirtualCatalogSchemaEntryBase;

class VirtualCatalogBase : public DuckCatalog {
public:
	explicit VirtualCatalogBase(AttachedDatabase &db);
	~VirtualCatalogBase() override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

protected:
	//! The subclass's wrapper around `target_schema`. Called under wrappers_lock.
	virtual unique_ptr<VirtualCatalogSchemaEntryBase> CreateSchemaWrapper(SchemaCatalogEntry &target_schema) = 0;

	//! Refuse a DROP SCHEMA that would strand entries the subclass still serves out of `schema_name`.
	//! Default is to allow it.
	virtual void ThrowIfSchemaStillServed(const string &schema_name) {
	}

private:
	VirtualCatalogSchemaEntryBase &GetOrCreateWrapper(SchemaCatalogEntry &target_schema);
	void DropSchema(ClientContext &context, DropInfo &info) override;

	mutex wrappers_lock;
	unordered_map<string, unique_ptr<VirtualCatalogSchemaEntryBase>> wrappers;
	// A wrapper whose DuckSchemaEntry went away (DROP SCHEMA, CREATE OR REPLACE, an MVCC rollback)
	// is retired rather than freed: a binder can be holding a raw SchemaCatalogEntry* to it for the
	// rest of the statement, the same reason the subclasses retire their cached entries.
	vector<unique_ptr<VirtualCatalogSchemaEntryBase>> retired_wrappers;
};

} // namespace duckdb
