#pragma once

#include "duckdb.hpp"

#include "crossing.hpp"

namespace duckdb {

struct PolicyName {
	string catalog;
	string schema;
	string name;

	string ToString() const;
};

struct PolicyVerb {
	bool granted = false;
	string using_text;
	string check_text;
};

struct PolicyKey {
	vector<string> columns;
	string columns_from_query;
	string verify_unique;
};

struct PolicyTable {
	PolicyName name;
	//! Where the table came from; the base of every error or issue path about it.
	string path;
	vector<PolicyVerb> verbs = vector<PolicyVerb>(CrossingVerbs().size());
	bool has_key = false;
	PolicyKey key;
	vector<PolicyName> foreign_keys;
};

struct PolicyFunctions {
	//! False when the whitelist was set exactly, dropping the built-in default.
	bool builtins = true;
	vector<string> add;
	vector<string> remove;
};

struct PolicySchema {
	PolicyName name;
	string path;
};

struct PolicyConfig {
	vector<PolicySchema> create_schemas;
	vector<PolicyTable> tables;
	PolicyFunctions functions;
};

enum class PolicyListOp : uint8_t { ADD, REMOVE, SET };

enum class PolicyEditKind : uint8_t { VERB, CREATE, PRIMARY_KEY, FOREIGN_KEYS, WHITELIST };

struct PolicyEdit {
	PolicyEditKind kind;
	string path;
	PolicyName target;
	CrossingVerb verb = CrossingVerb::SELECT;
	PolicyVerb verb_value;
	//! PRIMARY_KEY: false when the key is unset back to the discovered one.
	bool has_key = false;
	PolicyKey key;
	PolicyListOp op = PolicyListOp::SET;
	vector<PolicyName> foreign_keys;
	vector<string> functions;
};

//! Strict SQL identifier components: `a.b`, `"a.b".c`, `"say ""hi"""`. No empty parts; unquoted parts cannot be
//! blank-padded.
vector<string> ParsePolicyNameParts(const string &text);
PolicyName ParsePolicyTableName(const string &text);
PolicyName ParsePolicySchemaName(const string &text);

PolicyConfig ParsePolicyJson(const string &json);

//! `op` is null for the four-argument form.
PolicyEdit ParsePolicyEdit(const string &target, const string &setting, const string *op, const Value &value);

} // namespace duckdb
