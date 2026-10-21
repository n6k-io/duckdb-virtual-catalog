#pragma once

#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

//! Built-ins a bridge pushes to the source unless its owner disallows them.
const case_insensitive_set_t &PushableBuiltins();
//! Built-ins that read or change the instance, session or transaction they run in, so a pushed call would act
//! on the source. Every internal scalar or aggregate is in exactly one of the two lists; only
//! test_builtin_functions.cpp enforces this.
const case_insensitive_set_t &TargetOnlyBuiltins();
//! Settings the source reads again when it rebinds or runs a pushed plan, so source and target must agree on them.
//! Checked once at ATTACH, which fails on a mismatch; a later SET on either side is not checked.
const case_insensitive_set_t &MatchedSettings();

} // namespace duckdb
