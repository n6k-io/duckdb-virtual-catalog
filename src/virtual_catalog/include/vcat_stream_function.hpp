#pragma once

#include "duckdb/function/table_function.hpp"

#include <string>

namespace duckdb {

class ExtensionLoader;

namespace vcat {

// A generator in the host process, reached through three scalar UDFs it registered on the served
// connection:
//
//   open(handle, function, args_json, input_ipc BLOB) -> BLOB   the Arrow IPC schema message
//   next(handle)                                      -> BLOB   one IPC record batch, NULL at end
//   close(handle)                                     -> BLOB   trailing end-of-stream bytes
//
// Carried on TableFunction::function_info, so a host stream is an ordinary catalog entry.
struct StreamFunctionInfo : public TableFunctionInfo {
	// What the host dispatches on; need not match the catalog entry's name.
	std::string function_name;
	std::string open_udf;
	std::string next_udf;
	std::string close_udf;

	StreamFunctionInfo(std::string function_p, std::string open_p, std::string next_p, std::string close_p)
	    : function_name(std::move(function_p)), open_udf(std::move(open_p)), next_udf(std::move(next_p)),
	      close_udf(std::move(close_p)) {
	}
};

// Registers vcat_create_stream_function / vcat_drop_stream_function / vcat_stream_functions.
void RegisterStreamFunctionCreators(ExtensionLoader &loader);

} // namespace vcat
} // namespace duckdb
