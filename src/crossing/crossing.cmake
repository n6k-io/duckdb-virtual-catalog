set(CROSSING_ROOT ${CMAKE_CURRENT_LIST_DIR})

set(CROSSING_ENGINE_SOURCE_NAMES
    crossing.cpp
    fragment.cpp
    seam.cpp
    source.cpp
    source_evaluation.cpp
    rules.cpp
    labelling.cpp
    fold.cpp
    filter_halves.cpp
    pass.cpp
    seam_split.cpp
    scan_columns.cpp
    table_indices.cpp)

set(CROSSING_LIBRARY_SOURCE_NAMES
    ${CROSSING_ENGINE_SOURCE_NAMES}
    vcat_catalog_base.cpp
    vcat_schema_entry_base.cpp
    vcat_permissions.cpp
    vcat_describe.cpp
    crossing_register.cpp
    crossing_catalog.cpp
    crossing_table_entry.cpp
    crossing_scan.cpp
    crossing_write.cpp
    crossing_shape.cpp
    crossing_pass.cpp
    crossing_transactions.cpp)

function(crossing_engine_sources out_var)
  set(result "")
  foreach(name ${CROSSING_ENGINE_SOURCE_NAMES})
    list(APPEND result ${CROSSING_ROOT}/${name})
  endforeach()
  set(${out_var} ${result} PARENT_SCOPE)
endfunction()

function(crossing_sources out_var)
  set(result "")
  foreach(name ${CROSSING_LIBRARY_SOURCE_NAMES})
    list(APPEND result ${CROSSING_ROOT}/${name})
  endforeach()
  set(${out_var} ${result} PARENT_SCOPE)
endfunction()

function(crossing_includes out_var)
  set(${out_var} ${CROSSING_ROOT}/include PARENT_SCOPE)
endfunction()
