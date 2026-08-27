#include "vcat_catalog.hpp"
#include "vcat_schema_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

VirtualCatalog::VirtualCatalog(AttachedDatabase &db) : DuckCatalog(db) {
}

VirtualCatalog::~VirtualCatalog() = default;

VirtualCatalogSchemaEntry &VirtualCatalog::GetOrCreateWrapper(SchemaCatalogEntry &target_schema) {
	lock_guard<mutex> lock(wrappers_lock);
	auto it = wrappers.find(target_schema.name);
	if (it != wrappers.end()) {
		return *it->second;
	}
	auto wrapper = make_uniq<VirtualCatalogSchemaEntry>(*this, target_schema);
	auto &ref = *wrapper;
	wrappers[target_schema.name] = std::move(wrapper);
	return ref;
}

optional_ptr<SchemaCatalogEntry> VirtualCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	auto inner = DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
	if (!inner) {
		return nullptr;
	}
	return &GetOrCreateWrapper(*inner);
}

void VirtualCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	DuckCatalog::ScanSchemas(context, [&](SchemaCatalogEntry &inner) { callback(GetOrCreateWrapper(inner)); });
}

static unique_ptr<Catalog> VirtualCatalogStorageAttach(optional_ptr<StorageExtensionInfo> /*storage_info*/,
                                                       ClientContext & /*context*/, AttachedDatabase &db,
                                                       const string & /*name*/, AttachInfo & /*info*/,
                                                       AttachOptions & /*options*/) {
	return make_uniq<VirtualCatalog>(db);
}

static unique_ptr<TransactionManager>
VirtualCatalogStorageTransactionManager(optional_ptr<StorageExtensionInfo> /*storage_info*/, AttachedDatabase &db,
                                        Catalog & /*catalog*/) {
	return make_uniq<DuckTransactionManager>(db);
}

shared_ptr<StorageExtension> VirtualCatalog::CreateStorageExtension() {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = VirtualCatalogStorageAttach;
	ext->create_transaction_manager = VirtualCatalogStorageTransactionManager;
	return ext;
}

} // namespace duckdb
