cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME virtual_catalog_provider)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# See src/bridge/CMakeLists.txt for why the standard is pinned here rather than
# after the targets are created.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Normalised: an unresolved `src/provider/../..` leaves `src/` in third_party
# paths, which clang-tidy's `src/.*/` header filter then lints.
get_filename_component(REPO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../.. ABSOLUTE)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${REPO_ROOT}/src/common/include)
# nanoarrow is provider-only: the bridge moves DataChunks and never touches
# Arrow.
include_directories(${REPO_ROOT}/third_party/nanoarrow)

# src/common is compiled in, not linked against: the two extensions are separate
# binaries and neither depends on the other being loaded.
set(EXTENSION_SOURCES
    ${REPO_ROOT}/third_party/nanoarrow/nanoarrow.c
    ${REPO_ROOT}/third_party/nanoarrow/nanoarrow_ipc.c
    ${REPO_ROOT}/third_party/nanoarrow/flatcc.c
    ${REPO_ROOT}/src/common/vcat_catalog_base.cpp
    ${REPO_ROOT}/src/common/vcat_schema_entry_base.cpp
    ${REPO_ROOT}/src/common/vcat_permissions.cpp
    ${REPO_ROOT}/src/common/vcat_describe.cpp
    provider_extension.cpp
    provider_virtual_catalog.cpp
    provider_arrow.cpp
    provider_table_register.cpp
    provider_table_catalog.cpp
    provider_table_entry.cpp
    provider_table_insert.cpp
    provider_table_update.cpp
    provider_table_delete.cpp
    arrow_ipc_encode.cpp
    arrow_frame_decoder.cpp
    vcat_stream_function.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# yyjson is provider-only too: it carries the ALTER details and the
# stream-function metadata across the UDF boundary.
target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

option(VCAT_BUILD_CPP_TESTS "Build the virtual_catalog C++ unit tests" ON)
if(VCAT_BUILD_CPP_TESTS)
  file(GLOB_RECURSE VCAT_PROVIDER_TEST_SOURCES CONFIGURE_DEPENDS
       ${REPO_ROOT}/test/cpp/provider/test_*.cpp)
  add_executable(
    vcat_provider_unittest ${REPO_ROOT}/test/cpp/provider/unittest_main.cpp
                           ${VCAT_PROVIDER_TEST_SOURCES})
  target_include_directories(
    vcat_provider_unittest PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch
                                   ${REPO_ROOT}/test/cpp/provider)
  target_link_libraries(vcat_provider_unittest duckdb_static duckdb_yyjson
                        Threads::Threads)
  link_extension_libraries(vcat_provider_unittest "")
  set_target_properties(vcat_provider_unittest
                        PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
