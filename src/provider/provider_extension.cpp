#include "vcatprovider_extension.hpp"

#include "provider_info.hpp"
#include "provider_virtual_catalog.hpp"
#include "vcat_describe.hpp"
#include "vcat_permissions.hpp"
#include "vcat_stream_function.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

void VcatproviderLoadInternal(ExtensionLoader &loader) {
	RegisterProviderFunctions(loader);
	RegisterTablePermissionsFunction(loader, "vcatprovider_table_permissions");
	RegisterTableDescribeFunction(loader, "vcatprovider_table_describe");
	vcat::RegisterStreamFunctionCreators(loader);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, VCATPROVIDER_TYPE, VirtualCatalogProvider::CreateStorageExtension());
}

void VcatproviderExtension::Load(ExtensionLoader &loader) {
	VcatproviderLoadInternal(loader);
}

std::string VcatproviderExtension::Name() {
	return "vcatprovider";
}

std::string VcatproviderExtension::Version() const {
#ifdef EXT_VERSION_VCATPROVIDER
	return EXT_VERSION_VCATPROVIDER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(vcatprovider, loader) {
	duckdb::VcatproviderLoadInternal(loader);
}
}
