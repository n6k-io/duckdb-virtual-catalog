#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VcatproviderExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void VcatproviderLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
