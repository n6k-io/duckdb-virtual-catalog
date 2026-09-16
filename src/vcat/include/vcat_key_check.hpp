#pragma once

#include "duckdb.hpp"

namespace duckdb {

inline string DescribeKeyColumns(const vector<string> &pk_cols) {
	string out;
	for (idx_t i = 0; i < pk_cols.size(); i++) {
		if (i > 0) {
			out += ", ";
		}
		out += pk_cols[i];
	}
	return out;
}

// Every row counted in `keys_sent` is itself written, so `affected` exceeds it only when a key
// reached a row the scan did not name -- one the select policy hid. Naming the value would widen
// the write-path existence oracle: the count says a hidden row exists, the value would say which.
inline void EnsureKeyIsUnique(idx_t affected, idx_t keys_sent, const string &table, const vector<string> &pk_cols,
                              const char *verb, bool rolled_back) {
	if (affected <= keys_sent) {
		return;
	}
	throw ConstraintException(
	    "virtual_catalog: %s on '%s' matched %llu source rows for %llu key value(s); the declared primary key (%s) is "
	    "not unique on the source. %s",
	    verb, table, static_cast<uint64_t>(affected), static_cast<uint64_t>(keys_sent), DescribeKeyColumns(pk_cols),
	    rolled_back ? "No changes were applied." : "The write was already sent to the provider.");
}

} // namespace duckdb
