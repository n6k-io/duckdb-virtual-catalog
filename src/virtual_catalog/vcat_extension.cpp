#include "virtual_catalog_extension.hpp"
#include "bridge_bridge.hpp"
#include "vcat_stream_function.hpp"
#include "bridge_table_function.hpp"
#include "vcat_catalog.hpp"
#include "provider_info.hpp"
#include "vcat_describe.hpp"
#include "vcat_permissions.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

void VirtualCatalogLoadInternal(ExtensionLoader &loader) {
	RegisterBridgeTableFunction(loader);
	RegisterBridgeFunctions(loader);
	RegisterProviderFunctions(loader);
	RegisterTablePermissionsFunction(loader);
	RegisterTableDescribeFunction(loader);
	vcat::RegisterStreamFunctionCreators(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "virtual_catalog", VirtualCatalog::CreateStorageExtension());
}

void VirtualCatalogExtension::Load(ExtensionLoader &loader) {
	VirtualCatalogLoadInternal(loader);
}

std::string VirtualCatalogExtension::Name() {
	return "virtual_catalog";
}

std::string VirtualCatalogExtension::Version() const {
#ifdef EXT_VERSION_VIRTUAL_CATALOG
	return EXT_VERSION_VIRTUAL_CATALOG;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(virtual_catalog, loader) {
	duckdb::VirtualCatalogLoadInternal(loader);
}
}
