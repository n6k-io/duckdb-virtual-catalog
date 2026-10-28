cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME vcatbridge)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(REPO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../.. ABSOLUTE)

include(${REPO_ROOT}/third_party/crossing/crossing.cmake)
include(${REPO_ROOT}/src/vcat/vcat.cmake)
crossing_sources(CROSSING_SOURCES)
crossing_includes(CROSSING_INCLUDES)
vcat_sources(VCAT_SOURCES)
vcat_includes(VCAT_INCLUDES)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CROSSING_INCLUDES} ${VCAT_INCLUDES})

set(EXTENSION_SOURCES
    ${CROSSING_SOURCES}
    ${VCAT_SOURCES}
    bridge_extension.cpp
    bridge_catalog.cpp
    builtin_functions.cpp
    duckdb_source.cpp
    lockdown.cpp
    policy_config.cpp
    predicate_fold.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

option(VCAT_BUILD_CPP_TESTS "Build the virtual_catalog C++ unit tests" ON)
if(VCAT_BUILD_CPP_TESTS AND BUILD_MAIN_DUCKDB_LIBRARY)
  file(GLOB_RECURSE VCAT_BRIDGE_TEST_SOURCES CONFIGURE_DEPENDS
       ${REPO_ROOT}/test/cpp/bridge/test_*.cpp)
  add_executable(
    vcat_bridge_unittest ${REPO_ROOT}/test/cpp/bridge/unittest_main.cpp
                         ${VCAT_BRIDGE_TEST_SOURCES})
  target_include_directories(
    vcat_bridge_unittest
    PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch
            ${REPO_ROOT}/test/cpp/bridge ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_link_libraries(vcat_bridge_unittest duckdb_static Threads::Threads)
  link_extension_libraries(vcat_bridge_unittest "")
  set_target_properties(vcat_bridge_unittest PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                        ${CMAKE_BINARY_DIR})
endif()
