set(VCAT_ROOT ${CMAKE_CURRENT_LIST_DIR})

set(VCAT_SOURCE_NAMES vcat_catalog_base.cpp vcat_schema_entry_base.cpp
                      vcat_permissions.cpp vcat_describe.cpp)

function(vcat_sources out_var)
  set(result "")
  foreach(name ${VCAT_SOURCE_NAMES})
    list(APPEND result ${VCAT_ROOT}/${name})
  endforeach()
  set(${out_var}
      ${result}
      PARENT_SCOPE)
endfunction()

function(vcat_includes out_var)
  set(${out_var}
      ${VCAT_ROOT}/include
      PARENT_SCOPE)
endfunction()
