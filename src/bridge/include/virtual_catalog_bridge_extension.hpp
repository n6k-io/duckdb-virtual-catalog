#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VirtualCatalogBridgeExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void VirtualCatalogBridgeLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
