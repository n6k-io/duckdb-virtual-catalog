#include "virtual_catalog_provider_extension.hpp"

#include "provider_info.hpp"
#include "provider_virtual_catalog.hpp"
#include "vcat_describe.hpp"
#include "vcat_permissions.hpp"
#include "vcat_stream_function.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

void VirtualCatalogProviderLoadInternal(ExtensionLoader &loader) {
	RegisterProviderFunctions(loader);
	RegisterTablePermissionsFunction(loader, "provider_table_permissions");
	RegisterTableDescribeFunction(loader, "provider_table_describe");
	vcat::RegisterStreamFunctionCreators(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, VIRTUAL_CATALOG_PROVIDER_TYPE, VirtualCatalogProvider::CreateStorageExtension());
}

void VirtualCatalogProviderExtension::Load(ExtensionLoader &loader) {
	VirtualCatalogProviderLoadInternal(loader);
}

std::string VirtualCatalogProviderExtension::Name() {
	return "virtual_catalog_provider";
}

std::string VirtualCatalogProviderExtension::Version() const {
#ifdef EXT_VERSION_VIRTUAL_CATALOG_PROVIDER
	return EXT_VERSION_VIRTUAL_CATALOG_PROVIDER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(virtual_catalog_provider, loader) {
	duckdb::VirtualCatalogProviderLoadInternal(loader);
}
}
