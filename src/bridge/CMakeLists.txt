cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME virtual_catalog_bridge)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(REPO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../.. ABSOLUTE)

include(${REPO_ROOT}/src/crossing/crossing.cmake)
crossing_sources(CROSSING_SOURCES)
crossing_includes(CROSSING_INCLUDES)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CROSSING_INCLUDES})

set(EXTENSION_SOURCES ${CROSSING_SOURCES} bridge_extension.cpp
                      duckdb_source.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
