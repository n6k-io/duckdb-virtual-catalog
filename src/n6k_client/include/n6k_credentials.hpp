#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;

// Strip a known scheme (http/https/n6k/n6ks) to a host[:port][/path] key, matching how secret SCOPEs are normalized.
string StripUrlScheme(const string &url);

// Token at ATTACH time. Precedence: inline `token` > TYPE n6k secret > "".
// The only place a credential is resolved: the token is handed to the session at ATTACH and
// travels on the WebSocket from then on, so there is no per-request resolution.
string ResolveAttachToken(ClientContext &context, const string &base_url, const string &inline_token);

// Percent-encode for an x-www-form-urlencoded body. Native-only.
string FormUrlEncode(const string &value);

} // namespace duckdb
