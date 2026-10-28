#include "vcatbridge_extension.hpp"

#include "bridge_catalog.hpp"
#include "duckdb_source.hpp"
#include "lockdown.hpp"
#include "vcat_describe.hpp"
#include "vcat_permissions.hpp"

namespace duckdb {

void VcatbridgeLoadInternal(ExtensionLoader &loader) {
	RegisterTablePermissionsFunction(loader, "bridge_table_permissions");
	RegisterTableDescribeFunction(loader, "bridge_table_describe");
	RegisterBridgeFunctions(loader);
	RegisterLockdownFunction(loader);
	RegisterBridgeCatalog(loader);
}

void VcatbridgeExtension::Load(ExtensionLoader &loader) {
	VcatbridgeLoadInternal(loader);
}

std::string VcatbridgeExtension::Name() {
	return "vcatbridge";
}

std::string VcatbridgeExtension::Version() const {
#ifdef EXT_VERSION_VCATBRIDGE
	return EXT_VERSION_VCATBRIDGE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(vcatbridge, loader) {
	duckdb::VcatbridgeLoadInternal(loader);
}
}
