#include "virtual_catalog_bridge_extension.hpp"

#include "crossing.hpp"
#include "crossing_pass.hpp"
#include "duckdb_source.hpp"
#include "vcat_describe.hpp"
#include "vcat_permissions.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

void VirtualCatalogBridgeLoadInternal(ExtensionLoader &loader) {
	RegisterTablePermissionsFunction(loader, "bridge_table_permissions");
	RegisterTableDescribeFunction(loader, "bridge_table_describe");
	RegisterBridgeFunctions(loader);
	CrossingSource::Register(loader, VIRTUAL_CATALOG_BRIDGE_TYPE, RedeemBridgeAttach);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	OptimizerExtension crossing;
	crossing.pre_optimize_function = CrossingMoveWorkPass;
	crossing.optimize_function = CrossingNarrowPass;
	OptimizerExtension::Register(config, crossing);
}

void VirtualCatalogBridgeExtension::Load(ExtensionLoader &loader) {
	VirtualCatalogBridgeLoadInternal(loader);
}

std::string VirtualCatalogBridgeExtension::Name() {
	return "virtual_catalog_bridge";
}

std::string VirtualCatalogBridgeExtension::Version() const {
#ifdef EXT_VERSION_VIRTUAL_CATALOG_BRIDGE
	return EXT_VERSION_VIRTUAL_CATALOG_BRIDGE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(virtual_catalog_bridge, loader) {
	duckdb::VirtualCatalogBridgeLoadInternal(loader);
}
}
