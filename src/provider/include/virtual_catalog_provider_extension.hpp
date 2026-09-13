#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VirtualCatalogProviderExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void VirtualCatalogProviderLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
