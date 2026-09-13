cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME virtual_catalog)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Pin the C++ standard before build_*_extension create the targets (the standard
# is captured into each target's CXX_STANDARD property at creation time, so
# setting it afterwards is a no-op). _REQUIRED ON makes an unsupported standard
# a hard error instead of silently decaying to an older one. Pinned at 14 so a
# local build fails the same way CI's pre-C++17 toolchain would, catching
# accidental C++17 usage early.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/virtual_catalog/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/nanoarrow)

set(EXTENSION_SOURCES
  third_party/nanoarrow/nanoarrow.c
  third_party/nanoarrow/nanoarrow_ipc.c
  third_party/nanoarrow/flatcc.c
  src/virtual_catalog/vcat_permissions.cpp
  src/virtual_catalog/vcat_describe.cpp
  src/virtual_catalog/arrow_ipc_encode.cpp
  src/virtual_catalog/arrow_frame_decoder.cpp
  src/virtual_catalog/vcat_extension.cpp
  src/virtual_catalog/bridge_bridge.cpp
  src/virtual_catalog/bridge_plan.cpp
  src/virtual_catalog/bridge_write_plan.cpp
  src/virtual_catalog/bridge_seam.cpp
  src/virtual_catalog/bridge_scan.cpp
  src/virtual_catalog/bridge_pushdown.cpp
  src/virtual_catalog/bridge_pushdown_rules.cpp
  src/virtual_catalog/bridge_merger.cpp
  src/virtual_catalog/bridge_policy.cpp
  src/virtual_catalog/bridge_catalog.cpp
  src/virtual_catalog/bridge_insert.cpp
  src/virtual_catalog/bridge_update.cpp
  src/virtual_catalog/bridge_delete.cpp
  src/virtual_catalog/bridge_table_entry.cpp
  src/virtual_catalog/vcat_stream_function.cpp
  src/virtual_catalog/bridge_transaction.cpp
  src/virtual_catalog/vcat_catalog.cpp
  src/virtual_catalog/vcat_schema_entry.cpp
  src/virtual_catalog/provider_arrow.cpp
  src/virtual_catalog/provider_table_register.cpp
  src/virtual_catalog/provider_table_catalog.cpp
  src/virtual_catalog/provider_table_entry.cpp
  src/virtual_catalog/provider_table_insert.cpp
  src/virtual_catalog/provider_table_update.cpp
  src/virtual_catalog/provider_table_delete.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# C++ unit tests. Out-of-tree extensions get no hook into duckdb's own `unittest`
# binary, so this is a standalone Catch runner over the same static extension lib.
option(VCAT_BUILD_CPP_TESTS "Build the virtual_catalog C++ unit tests" ON)
if(VCAT_BUILD_CPP_TESTS AND BUILD_MAIN_DUCKDB_LIBRARY)
  # CONFIGURE_DEPENDS: a newly added test_*.cpp is picked up by the next build
  # without a manual reconfigure.
  file(GLOB_RECURSE VCAT_TEST_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/test/cpp/test_*.cpp)
  add_executable(vcat_unittest test/cpp/unittest_main.cpp ${VCAT_TEST_SOURCES})
  # test/cpp so `framework/...` includes resolve from any depth under it.
  target_include_directories(vcat_unittest PRIVATE
    ${DUCKDB_MODULE_BASE_DIR}/third_party/catch
    ${CMAKE_CURRENT_SOURCE_DIR}/test/cpp)
  target_link_libraries(vcat_unittest duckdb_static duckdb_yyjson Threads::Threads)
  # Pulls in the generated loader plus every statically linked extension
  # (parquet, core_functions, and this one), same as duckdb's own unittest.
  link_extension_libraries(vcat_unittest "")
  set_target_properties(vcat_unittest PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()
