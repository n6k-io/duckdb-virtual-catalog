#include "crossing.hpp"
#include "crossing_catalog.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

namespace {

//! The factory reaches attach through here: StorageExtension::attach is a plain function pointer,
//! so a capturing lambda cannot be one.
struct CrossingStorageInfo : public StorageExtensionInfo {
	CrossingStorageInfo(string type_p, CrossingSource::Factory factory_p)
	    : type(std::move(type_p)), factory(std::move(factory_p)) {
	}

	string type;
	CrossingSource::Factory factory;
};

unique_ptr<Catalog> AttachCrossingCatalog(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &, AttachInfo &info, AttachOptions &) {
	auto &crossing_info = static_cast<CrossingStorageInfo &>(*storage_info);
	unique_ptr<CrossingSource> source;
	if (crossing_info.factory) {
		source = crossing_info.factory(context, info);
		if (!source) {
			throw IOException("virtual_catalog_bridge: the factory for TYPE %s returned no source for '%s'",
			                  crossing_info.type, info.path);
		}
	}
	return make_uniq<CrossingCatalog>(db, crossing_info.type, std::move(source));
}

unique_ptr<TransactionManager> CreateCrossingTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                AttachedDatabase &db, Catalog &) {
	// DuckDB's own manager: the target's transactions are ordinary ones. The source is resolved
	// alongside them through CrossingSource::Begin/Commit/Rollback, not from here.
	return make_uniq<DuckTransactionManager>(db);
}

} // namespace

void CrossingSource::Register(ExtensionLoader &loader, const string &type, Factory factory) {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = AttachCrossingCatalog;
	ext->create_transaction_manager = CreateCrossingTransactionManager;
	ext->storage_info = make_uniq<CrossingStorageInfo>(type, std::move(factory));

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, type, std::move(ext));
}

} // namespace duckdb
