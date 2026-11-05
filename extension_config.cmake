# This file is included by DuckDB's build system. It specifies which extensions to load

file(READ ${CMAKE_CURRENT_LIST_DIR}/VERSION VCAT_VERSION)
string(STRIP "${VCAT_VERSION}" VCAT_VERSION)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${CMAKE_CURRENT_LIST_DIR}/VERSION)

duckdb_extension_load(vcatbridge
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/bridge
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/bridge/include
    TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test/sql/bridge
    EXTENSION_VERSION "v${VCAT_VERSION}"
    LOAD_TESTS
)
duckdb_extension_statically_link(vcatbridge)

duckdb_extension_load(vcatprovider
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/provider
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/provider/include
    TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test/sql/provider
    EXTENSION_VERSION "v${VCAT_VERSION}"
    LOAD_TESTS
)
duckdb_extension_statically_link(vcatprovider)
