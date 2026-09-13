cmake_minimum_required(VERSION 3.5)

project(crossing)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(REPO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..)

set(DUCKDB_BUILD_DIR
    "${REPO_ROOT}/build/release"
    CACHE PATH "Directory of a configured duckdb build, for libduckdb_static.a")

set(DUCKDB_STATIC_LIB ${DUCKDB_BUILD_DIR}/src/libduckdb_static.a)
set(DUCKDB_LOADER_LIB
    ${DUCKDB_BUILD_DIR}/extension/libdummy_static_extension_loader.a)

foreach(lib ${DUCKDB_STATIC_LIB} ${DUCKDB_LOADER_LIB})
  if(NOT EXISTS ${lib})
    message(FATAL_ERROR "missing ${lib} -- run `make release` first")
  endif()
endforeach()

include(${CMAKE_CURRENT_SOURCE_DIR}/sources.cmake)

set(CROSSING_ENGINE_SOURCES "")
foreach(engine_source ${CROSSING_ENGINE_SOURCE_NAMES})
  list(APPEND CROSSING_ENGINE_SOURCES
       ${CMAKE_CURRENT_SOURCE_DIR}/${engine_source})
endforeach()

file(GLOB_RECURSE CROSSING_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/test_*.cpp)

add_executable(
  crossing_unittest test/unittest_main.cpp ${CROSSING_ENGINE_SOURCES}
                    ${CROSSING_TEST_SOURCES})

target_include_directories(
  crossing_unittest
  PRIVATE ${REPO_ROOT}/duckdb/src/include ${REPO_ROOT}/duckdb/third_party/catch
          ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/include
          ${CMAKE_CURRENT_SOURCE_DIR}/test)

find_package(Threads REQUIRED)
target_link_libraries(crossing_unittest ${DUCKDB_STATIC_LIB}
                      ${DUCKDB_LOADER_LIB} Threads::Threads)
