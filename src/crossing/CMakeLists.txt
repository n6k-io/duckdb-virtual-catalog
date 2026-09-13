cmake_minimum_required(VERSION 3.5)

project(crossing)

include(${CMAKE_CURRENT_SOURCE_DIR}/crossing.cmake)

if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  return()
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DUCKDB_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/../../duckdb"
    CACHE PATH "Directory of the duckdb source tree")
set(DUCKDB_BUILD_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/../../build/release"
    CACHE PATH "Directory of a configured duckdb build, for libduckdb_static.a")

set(DUCKDB_STATIC_LIB ${DUCKDB_BUILD_DIR}/src/libduckdb_static.a)
set(DUCKDB_LOADER_LIB
    ${DUCKDB_BUILD_DIR}/extension/libdummy_static_extension_loader.a)

foreach(lib ${DUCKDB_STATIC_LIB} ${DUCKDB_LOADER_LIB})
  if(NOT EXISTS ${lib})
    message(
      FATAL_ERROR "missing ${lib} -- set DUCKDB_BUILD_DIR to a built duckdb")
  endif()
endforeach()

crossing_engine_sources(CROSSING_ENGINE_SOURCES)
crossing_includes(CROSSING_INCLUDES)

file(GLOB_RECURSE CROSSING_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/test_*.cpp)

add_executable(
  crossing_unittest test/unittest_main.cpp ${CROSSING_ENGINE_SOURCES}
                    ${CROSSING_TEST_SOURCES})

target_include_directories(
  crossing_unittest
  PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
          ${DUCKDB_SOURCE_DIR}/third_party/catch ${CMAKE_CURRENT_SOURCE_DIR}
          ${CROSSING_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/test)

find_package(Threads REQUIRED)
target_link_libraries(crossing_unittest ${DUCKDB_STATIC_LIB}
                      ${DUCKDB_LOADER_LIB} Threads::Threads)
