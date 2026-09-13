# This file is included by DuckDB's build system. It specifies which extensions to load

duckdb_extension_load(virtual_catalog_bridge
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/bridge
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/bridge/include
    TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test/sql/bridge
    LOAD_TESTS
)

duckdb_extension_load(virtual_catalog_provider
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/provider
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/provider/include
    TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test/sql/provider
    LOAD_TESTS
)
