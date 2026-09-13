PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=n6k_server
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB source is at the repo root
DUCKDB_SRCDIR=../../duckdb/

# Include the Makefile from extension-ci-tools
include ../../extension-ci-tools/makefiles/duckdb_extension.Makefile
