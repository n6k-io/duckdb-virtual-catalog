#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VcatbridgeExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void VcatbridgeLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
