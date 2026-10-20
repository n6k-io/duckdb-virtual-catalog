#pragma once

#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

//! Built-ins a bridge pushes to the source unless its owner disallows them.
const case_insensitive_set_t &PushableBuiltins();
//! Built-ins that read the instance, session or transaction they run in, so a pushed call would answer for the
//! source. Every internal scalar or aggregate is in exactly one of the two lists.
const case_insensitive_set_t &TargetOnlyBuiltins();

} // namespace duckdb
