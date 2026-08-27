#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VirtualCatalogExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void VirtualCatalogLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
