PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=virtual_catalog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Statements in src/ are built as parser nodes, not assembled from strings. See
# scripts/check_no_sql.py for what this looks for and what it deliberately does not.
check-no-sql:
	python3 scripts/check_no_sql.py src

# C++ unit tests (Catch). Builds only the unittest targets and their deps;
# does not need a prior `make release`.
CPP_TEST_BUILD_DIR=${PROJ_DIR}build/release
CPP_TEST_LOG=$(CPP_TEST_BUILD_DIR)/vcat_unittest_build.log
JOBS ?= $(shell sysctl -n hw.ncpu)
test-cpp:
	@if [ ! -f "$(CPP_TEST_BUILD_DIR)/CMakeCache.txt" ]; then $(MAKE) release; fi
	@cmake "$(CPP_TEST_BUILD_DIR)" > "$(CPP_TEST_LOG)" 2>&1 || { cat "$(CPP_TEST_LOG)"; exit 1; }
	@echo "building vcat_provider_unittest..."
	@cmake --build "$(CPP_TEST_BUILD_DIR)" --target vcat_provider_unittest -j$(JOBS) \
		>> "$(CPP_TEST_LOG)" 2>&1 || { cat "$(CPP_TEST_LOG)"; exit 1; }
	@"$(CPP_TEST_BUILD_DIR)/vcat_provider_unittest" $(CPP_TEST_ARGS)

PYTEST=uv run pytest test/python

BRIDGE_LOG=$(CPP_TEST_BUILD_DIR)/vcat_bridge_build.log
BRIDGE_TEST_ARGS?=test/sql/bridge/*
bridge:
	@if [ ! -f "$(CPP_TEST_BUILD_DIR)/CMakeCache.txt" ]; then $(MAKE) release; fi
	@echo "building virtual_catalog_bridge..."
	@cmake --build "$(CPP_TEST_BUILD_DIR)" -j$(JOBS) --target \
		virtual_catalog_bridge_loadable_extension unittest \
		> "$(BRIDGE_LOG)" 2>&1 || { cat "$(BRIDGE_LOG)"; exit 1; }

# One path end to end: its sqllogictests, then the python tests marked for it.
# The marker follows test/python/<path>/, plus the matching half of the parity probes.
test-bridge: bridge
	@"$(CPP_TEST_BUILD_DIR)/test/unittest" "$(BRIDGE_TEST_ARGS)"
	@$(PYTEST) -m bridge

PROVIDER_LOG=$(CPP_TEST_BUILD_DIR)/vcat_provider_build.log
PROVIDER_TEST_ARGS?=test/sql/provider/*
provider:
	@if [ ! -f "$(CPP_TEST_BUILD_DIR)/CMakeCache.txt" ]; then $(MAKE) release; fi
	@echo "building virtual_catalog_provider..."
	@cmake --build "$(CPP_TEST_BUILD_DIR)" -j$(JOBS) --target \
		virtual_catalog_provider_loadable_extension vcat_provider_unittest unittest \
		> "$(PROVIDER_LOG)" 2>&1 || { cat "$(PROVIDER_LOG)"; exit 1; }

test-provider: provider
	@"$(CPP_TEST_BUILD_DIR)/vcat_provider_unittest" $(CPP_TEST_ARGS)
	@"$(CPP_TEST_BUILD_DIR)/test/unittest" "$(PROVIDER_TEST_ARGS)"
	@$(PYTEST) -m provider

test-parity:
	@$(PYTEST) -m parity

.PHONY: test-cpp bridge test-bridge provider test-provider test-parity
