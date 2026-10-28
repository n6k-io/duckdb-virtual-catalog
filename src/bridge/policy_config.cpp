#include "policy_config.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include "yyjson.hpp"

namespace duckdb {

namespace {

constexpr int64_t POLICY_VERSION = 1;

//! JSON and SQL values both lower to this, so one parser serves both entry points.
struct RawValue {
	enum class Kind : uint8_t { NUL, BOOL, INTEGER, STRING, LIST, OBJECT };
	Kind kind = Kind::NUL;
	bool boolean = false;
	int64_t integer = 0;
	string text;
	vector<RawValue> items;
	vector<std::pair<string, RawValue>> fields;
};

[[noreturn]] void Fail(const string &path, const string &message) {
	throw IOException("vcatbridge: %s: %s", path, message);
}

string Describe(const RawValue &value) {
	switch (value.kind) {
	case RawValue::Kind::NUL:
		return "null";
	case RawValue::Kind::BOOL:
		return value.boolean ? "true" : "false";
	case RawValue::Kind::INTEGER:
		return "a number";
	case RawValue::Kind::STRING:
		return "a string";
	case RawValue::Kind::LIST:
		return "a list";
	default:
		return "an object";
	}
}

string QuoteKey(const string &key) {
	return "\"" + StringUtil::Replace(key, "\"", "\\\"") + "\"";
}

string Child(const string &path, const string &key) {
	auto plain = !key.empty() && key.find_first_of(".\" []") == string::npos;
	auto part = plain ? key : QuoteKey(key);
	return path.empty() ? part : path + "." + part;
}

RawValue FromJson(duckdb_yyjson::yyjson_val *val, const string &path) {
	RawValue out;
	if (duckdb_yyjson::yyjson_is_null(val)) {
		Fail(path, "null is not a value here; leave the key out instead");
	}
	if (duckdb_yyjson::yyjson_is_bool(val)) {
		out.kind = RawValue::Kind::BOOL;
		out.boolean = duckdb_yyjson::yyjson_get_bool(val);
		return out;
	}
	if (duckdb_yyjson::yyjson_is_str(val)) {
		out.kind = RawValue::Kind::STRING;
		out.text = string(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val));
		return out;
	}
	if (duckdb_yyjson::yyjson_is_arr(val)) {
		out.kind = RawValue::Kind::LIST;
		auto iter = duckdb_yyjson::yyjson_arr_iter_with(val);
		idx_t index = 0;
		while (auto item = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			out.items.push_back(FromJson(item, path + "[" + std::to_string(index++) + "]"));
		}
		return out;
	}
	if (duckdb_yyjson::yyjson_is_obj(val)) {
		out.kind = RawValue::Kind::OBJECT;
		case_insensitive_set_t seen;
		auto iter = duckdb_yyjson::yyjson_obj_iter_with(val);
		while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
			string name(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
			if (!seen.insert(name).second) {
				Fail(path, "the key " + QuoteKey(name) + " appears twice");
			}
			out.fields.emplace_back(name, FromJson(duckdb_yyjson::yyjson_obj_iter_get_val(key), Child(path, name)));
		}
		return out;
	}
	if (duckdb_yyjson::yyjson_is_int(val)) {
		out.kind = RawValue::Kind::INTEGER;
		out.integer = duckdb_yyjson::yyjson_get_sint(val);
		return out;
	}
	Fail(path, "fractional numbers are not a value here");
}

RawValue FromSql(const Value &value, const string &path) {
	RawValue out;
	if (value.IsNull()) {
		return out;
	}
	auto &type = value.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		out.kind = RawValue::Kind::BOOL;
		out.boolean = BooleanValue::Get(value);
		return out;
	case LogicalTypeId::VARCHAR:
		out.kind = RawValue::Kind::STRING;
		out.text = StringValue::Get(value);
		return out;
	case LogicalTypeId::LIST: {
		out.kind = RawValue::Kind::LIST;
		auto &children = ListValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			out.items.push_back(FromSql(children[i], path + "[" + std::to_string(i) + "]"));
		}
		return out;
	}
	case LogicalTypeId::STRUCT: {
		out.kind = RawValue::Kind::OBJECT;
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &name = StructType::GetChildName(type, i);
			out.fields.emplace_back(name, FromSql(children[i], Child(path, name)));
		}
		return out;
	}
	default:
		Fail(path, "expected a string, boolean, list or struct, not " + type.ToString());
	}
}

void ThrowIfUnknownFields(const RawValue &object, const string &path, const vector<string> &known) {
	for (auto &field : object.fields) {
		bool found = false;
		for (auto &name : known) {
			found = found || field.first == name;
		}
		if (!found) {
			Fail(path, "unknown key " + QuoteKey(field.first) + "; expected one of: " + StringUtil::Join(known, ", "));
		}
	}
}

optional_ptr<const RawValue> Field(const RawValue &object, const string &name) {
	for (auto &field : object.fields) {
		if (field.first == name) {
			return &field.second;
		}
	}
	return nullptr;
}

bool IsBlank(string text) {
	StringUtil::Trim(text);
	return text.empty();
}

string NonEmptyString(const RawValue &value, const string &path, const string &what) {
	if (value.kind == RawValue::Kind::NUL) {
		Fail(path, what + " cannot be NULL");
	}
	if (value.kind != RawValue::Kind::STRING) {
		Fail(path, what + " must be a string, not " + Describe(value));
	}
	if (IsBlank(value.text)) {
		Fail(path, what + " cannot be empty");
	}
	return value.text;
}

vector<string> StringList(const RawValue &value, const string &path, const string &what, bool allow_single) {
	if (allow_single && value.kind == RawValue::Kind::STRING) {
		return {NonEmptyString(value, path, what)};
	}
	if (value.kind != RawValue::Kind::LIST) {
		Fail(path, what + " must be a list of strings, not " + Describe(value));
	}
	vector<string> out;
	for (idx_t i = 0; i < value.items.size(); i++) {
		out.push_back(NonEmptyString(value.items[i], path + "[" + std::to_string(i) + "]", what));
	}
	return out;
}

bool IsTrueText(string text) {
	StringUtil::Trim(text);
	return StringUtil::CIEquals(text, "true");
}

//! A USING or CHECK predicate: BOOL true becomes "true", false fails, anything else must be a non-empty string.
string Predicate(const RawValue &value, const string &path) {
	if (value.kind == RawValue::Kind::BOOL) {
		if (!value.boolean) {
			Fail(path, "a predicate cannot be false; leave the verb out to deny it");
		}
		return "true";
	}
	return NonEmptyString(value, path, "a predicate");
}

PolicyVerb ParseVerb(CrossingVerb verb, const RawValue &value, const string &path) {
	PolicyVerb out;
	if (value.kind == RawValue::Kind::NUL || (value.kind == RawValue::Kind::BOOL && !value.boolean)) {
		return out;
	}
	out.granted = true;
	out.using_text = "true";
	switch (verb) {
	case CrossingVerb::CREATE:
	case CrossingVerb::DROP:
		if (value.kind == RawValue::Kind::OBJECT || !IsTrueText(Predicate(value, path))) {
			Fail(path, string("'") + CrossingVerbName(verb) + "' takes only true");
		}
		return out;
	case CrossingVerb::INSERT:
		if (value.kind == RawValue::Kind::OBJECT) {
			ThrowIfUnknownFields(value, path, {"check"});
			auto check = Field(value, "check");
			if (!check) {
				Fail(path, "'insert' needs a check predicate");
			}
			out.check_text = Predicate(*check, Child(path, "check"));
			return out;
		}
		out.check_text = Predicate(value, path);
		return out;
	case CrossingVerb::UPDATE:
		if (value.kind == RawValue::Kind::OBJECT) {
			ThrowIfUnknownFields(value, path, {"using", "check"});
			auto using_value = Field(value, "using");
			auto check = Field(value, "check");
			if (using_value) {
				out.using_text = Predicate(*using_value, Child(path, "using"));
			}
			if (check) {
				out.check_text = Predicate(*check, Child(path, "check"));
			}
			return out;
		}
		out.using_text = Predicate(value, path);
		return out;
	default:
		if (value.kind == RawValue::Kind::OBJECT) {
			Fail(path, string("'") + CrossingVerbName(verb) + "' takes true or a predicate string");
		}
		out.using_text = Predicate(value, path);
		return out;
	}
}

PolicyKey ParseKey(const RawValue &value, const string &path) {
	PolicyKey key;
	if (value.kind == RawValue::Kind::LIST) {
		key.columns = StringList(value, path, "a key column", false);
		if (key.columns.empty()) {
			Fail(path, "the key column list cannot be empty");
		}
		return key;
	}
	if (value.kind != RawValue::Kind::OBJECT) {
		Fail(path, "primary_key must be a list of columns or an object, not " + Describe(value));
	}
	ThrowIfUnknownFields(value, path, {"columns", "columns_from_query", "verify_unique"});
	auto columns = Field(value, "columns");
	auto query = Field(value, "columns_from_query");
	auto verify = Field(value, "verify_unique");
	if (!columns == !query) {
		Fail(path, "primary_key needs exactly one of columns or columns_from_query");
	}
	if (columns) {
		key.columns = StringList(*columns, Child(path, "columns"), "a key column", false);
		if (key.columns.empty()) {
			Fail(Child(path, "columns"), "the key column list cannot be empty");
		}
	} else {
		key.columns_from_query = NonEmptyString(*query, Child(path, "columns_from_query"), "columns_from_query");
	}
	if (verify) {
		key.verify_unique = NonEmptyString(*verify, Child(path, "verify_unique"), "verify_unique");
	}
	return key;
}

PolicyName ParentName(const string &text, const PolicyName &child, const string &path) {
	PolicyName parent;
	try {
		parent = ParsePolicyTableName(text);
	} catch (std::exception &ex) {
		Fail(path, ErrorData(ex).RawMessage());
	}
	if (!StringUtil::CIEquals(parent.schema, child.schema)) {
		Fail(path, "'" + text + "' is in another schema; a foreign key never crosses schemas");
	}
	return parent;
}

vector<PolicyName> ParseForeignKeys(const vector<string> &names, const PolicyName &child, const string &path) {
	vector<PolicyName> out;
	for (auto &name : names) {
		out.push_back(ParentName(name, child, path));
	}
	return out;
}

bool TryParseVerbName(const string &text, CrossingVerb &out) {
	for (auto verb : CrossingVerbs()) {
		if (text == CrossingVerbName(verb)) {
			out = verb;
			return true;
		}
	}
	return false;
}

PolicyTable ParseTable(const string &name, const RawValue &value, const string &path) {
	PolicyTable table;
	table.path = path;
	try {
		table.name = ParsePolicyTableName(name);
	} catch (std::exception &ex) {
		Fail(path, ErrorData(ex).RawMessage());
	}
	if (value.kind != RawValue::Kind::OBJECT) {
		Fail(path, "a table entry must be an object, not " + Describe(value));
	}
	ThrowIfUnknownFields(value, path,
	                     {"select", "insert", "update", "delete", "alter", "drop", "primary_key", "foreign_keys"});
	bool any_verb = false;
	for (auto &field : value.fields) {
		auto field_path = Child(path, field.first);
		CrossingVerb verb;
		if (TryParseVerbName(field.first, verb)) {
			table.verbs[static_cast<idx_t>(verb)] = ParseVerb(verb, field.second, field_path);
			any_verb = any_verb || table.verbs[static_cast<idx_t>(verb)].granted;
		} else if (field.first == "primary_key") {
			table.has_key = true;
			table.key = ParseKey(field.second, field_path);
		} else {
			table.foreign_keys =
			    ParseForeignKeys(StringList(field.second, field_path, "a table name", false), table.name, field_path);
		}
	}
	if (!any_verb) {
		Fail(path, "grants no verb; add select, insert, update, delete, alter or drop");
	}
	return table;
}

PolicyFunctions ParseFunctions(const RawValue &value, const string &path) {
	PolicyFunctions functions;
	if (value.kind == RawValue::Kind::LIST) {
		functions.builtins = false;
		functions.add = StringList(value, path, "a function name", false);
		return functions;
	}
	if (value.kind != RawValue::Kind::OBJECT) {
		Fail(path,
		     "functions must be a list (the exact whitelist) or an object with add/remove, not " + Describe(value));
	}
	ThrowIfUnknownFields(value, path, {"add", "remove"});
	if (auto add = Field(value, "add")) {
		functions.add = StringList(*add, Child(path, "add"), "a function name", false);
	}
	if (auto remove = Field(value, "remove")) {
		functions.remove = StringList(*remove, Child(path, "remove"), "a function name", false);
	}
	return functions;
}

} // namespace

string PolicyName::ToString() const {
	string out = catalog.empty() ? "" : catalog + ".";
	out += schema;
	return name.empty() ? out : out + "." + name;
}

vector<string> ParsePolicyNameParts(const string &text) {
	vector<string> parts;
	string part;
	bool quoted_part = false;
	idx_t i = 0;
	const auto finish = [&]() {
		if (part.empty()) {
			throw IOException("vcatbridge: '%s' has an empty name part", text);
		}
		if (!quoted_part && (StringUtil::CharacterIsSpace(part.front()) || StringUtil::CharacterIsSpace(part.back()))) {
			throw IOException("vcatbridge: '%s' has spaces around a name part; quote it if they belong", text);
		}
		parts.push_back(std::move(part));
		part.clear();
		quoted_part = false;
	};
	while (i < text.size()) {
		auto c = text[i];
		if (c == '"' && part.empty() && !quoted_part) {
			quoted_part = true;
			i++;
			while (true) {
				if (i >= text.size()) {
					throw IOException("vcatbridge: '%s' has an unterminated quote", text);
				}
				if (text[i] == '"') {
					if (i + 1 < text.size() && text[i + 1] == '"') {
						part += '"';
						i += 2;
						continue;
					}
					i++;
					break;
				}
				part += text[i++];
			}
			if (part.empty()) {
				throw IOException("vcatbridge: '%s' has an empty name part", text);
			}
			if (i < text.size() && text[i] != '.') {
				throw IOException("vcatbridge: '%s' has text after a quoted name part", text);
			}
			continue;
		}
		if (c == '"') {
			throw IOException("vcatbridge: '%s' has a quote inside a name part", text);
		}
		if (c == '.') {
			finish();
			i++;
			if (i == text.size()) {
				throw IOException("vcatbridge: '%s' has an empty name part", text);
			}
			continue;
		}
		if (quoted_part) {
			throw IOException("vcatbridge: '%s' has text after a quoted name part", text);
		}
		part += c;
		i++;
	}
	finish();
	return parts;
}

PolicyName ParsePolicyTableName(const string &text) {
	auto parts = ParsePolicyNameParts(text);
	PolicyName name;
	if (parts.size() == 2) {
		name.schema = parts[0];
		name.name = parts[1];
	} else if (parts.size() == 3) {
		name.catalog = parts[0];
		name.schema = parts[1];
		name.name = parts[2];
	} else {
		throw IOException("vcatbridge: table name '%s' must be 'schema.table' or 'catalog.schema.table'", text);
	}
	return name;
}

PolicyName ParsePolicySchemaName(const string &text) {
	auto parts = ParsePolicyNameParts(text);
	if (parts.size() != 1) {
		throw IOException("vcatbridge: 'create' takes a schema name like 'main', not '%s'", text);
	}
	PolicyName name;
	name.schema = parts[0];
	return name;
}

PolicyConfig ParsePolicyJson(const string &json) {
	string text = json;
	duckdb_yyjson::yyjson_read_err error;
	auto doc =
	    duckdb_yyjson::yyjson_read_opts(&text[0], text.size(), duckdb_yyjson::YYJSON_READ_NOFLAG, nullptr, &error);
	if (!doc) {
		throw IOException("vcatbridge: the policy is not valid JSON: %s at byte %llu", error.msg,
		                  static_cast<uint64_t>(error.pos));
	}
	RawValue root;
	try {
		auto root_val = duckdb_yyjson::yyjson_doc_get_root(doc);
		if (!duckdb_yyjson::yyjson_is_obj(root_val)) {
			throw IOException("vcatbridge: the policy must be a JSON object");
		}
		root = FromJson(root_val, "");
	} catch (...) {
		duckdb_yyjson::yyjson_doc_free(doc);
		throw;
	}
	duckdb_yyjson::yyjson_doc_free(doc);

	ThrowIfUnknownFields(root, "policy", {"$schema", "policy_version", "schemas", "tables", "functions"});
	auto version = Field(root, "policy_version");
	if (!version) {
		Fail("policy", "policy_version is required; this extension supports policy_version 1");
	}
	if (version->kind != RawValue::Kind::INTEGER || version->integer != POLICY_VERSION) {
		Fail("policy_version", "unsupported version; this extension supports policy_version 1");
	}
	if (auto schema = Field(root, "$schema")) {
		NonEmptyString(*schema, "$schema", "$schema");
	}

	PolicyConfig config;
	if (auto schemas = Field(root, "schemas")) {
		if (schemas->kind != RawValue::Kind::OBJECT) {
			Fail("schemas", "must be an object of schema name to settings");
		}
		for (auto &entry : schemas->fields) {
			auto path = Child("schemas", entry.first);
			if (entry.second.kind != RawValue::Kind::OBJECT) {
				Fail(path, "a schema entry must be an object, not " + Describe(entry.second));
			}
			ThrowIfUnknownFields(entry.second, path, {"create"});
			auto create = Field(entry.second, "create");
			if (!create || !ParseVerb(CrossingVerb::CREATE, *create, Child(path, "create")).granted) {
				continue;
			}
			try {
				config.create_schemas.push_back({ParsePolicySchemaName(entry.first), path});
			} catch (std::exception &ex) {
				Fail(path, ErrorData(ex).RawMessage());
			}
		}
	}
	if (auto tables = Field(root, "tables")) {
		if (tables->kind != RawValue::Kind::OBJECT) {
			Fail("tables", "must be an object of table name to settings");
		}
		for (auto &entry : tables->fields) {
			config.tables.push_back(ParseTable(entry.first, entry.second, Child("tables", entry.first)));
		}
	}
	if (auto functions = Field(root, "functions")) {
		config.functions = ParseFunctions(*functions, "functions");
	}
	return config;
}

PolicyEdit ParsePolicyEdit(const string &target, const string &setting, const string *op, const Value &value) {
	PolicyEdit edit;
	edit.path = "'" + target + "' " + setting;
	auto raw = FromSql(value, edit.path);
	const auto require_op = [&](bool is_list) {
		if (is_list && !op) {
			Fail(edit.path, "is a list setting; pass an operation: vcatbridge_set_policy(id, '" + target + "', '" +
			                    setting + "', 'add' | 'remove' | 'set', value)");
		}
		if (!is_list && op) {
			Fail(edit.path, "takes a single value; call vcatbridge_set_policy(id, '" + target + "', '" + setting +
			                    "', value) without an operation");
		}
		if (!op) {
			return;
		}
		if (*op == "add") {
			edit.op = PolicyListOp::ADD;
		} else if (*op == "remove") {
			edit.op = PolicyListOp::REMOVE;
		} else if (*op == "set") {
			edit.op = PolicyListOp::SET;
		} else {
			Fail(edit.path, "unknown operation '" + *op + "'; expected add, remove or set");
		}
		if (raw.kind == RawValue::Kind::NUL) {
			Fail(edit.path, "the value cannot be NULL; use 'set' with an empty list to clear it");
		}
	};
	const auto names = [&]() {
		return StringList(raw, edit.path, "a name", edit.op != PolicyListOp::SET);
	};

	if (!target.empty() && target[0] == ':') {
		if (target != ":functions") {
			Fail(edit.path, "unknown target '" + target + "'; the only special target is ':functions'");
		}
		if (setting != "whitelist") {
			Fail(edit.path, "unknown setting; ':functions' has only 'whitelist'");
		}
		require_op(true);
		edit.kind = PolicyEditKind::WHITELIST;
		edit.functions = names();
		return edit;
	}

	if (setting == "create") {
		require_op(false);
		edit.kind = PolicyEditKind::CREATE;
		edit.target = ParsePolicySchemaName(target);
		edit.verb = CrossingVerb::CREATE;
		edit.verb_value = ParseVerb(CrossingVerb::CREATE, raw, edit.path);
		return edit;
	}
	edit.target = ParsePolicyTableName(target);
	if (TryParseVerbName(setting, edit.verb)) {
		require_op(false);
		edit.kind = PolicyEditKind::VERB;
		edit.verb_value = ParseVerb(edit.verb, raw, edit.path);
		return edit;
	}
	if (setting == "primary_key") {
		require_op(false);
		edit.kind = PolicyEditKind::PRIMARY_KEY;
		edit.has_key = raw.kind != RawValue::Kind::NUL;
		if (edit.has_key) {
			edit.key = ParseKey(raw, edit.path);
		}
		return edit;
	}
	if (setting == "foreign_keys") {
		require_op(true);
		edit.kind = PolicyEditKind::FOREIGN_KEYS;
		edit.foreign_keys = ParseForeignKeys(names(), edit.target, edit.path);
		return edit;
	}
	Fail(edit.path, "unknown setting; expected select, insert, update, delete, alter, drop, create, primary_key or "
	                "foreign_keys");
}

} // namespace duckdb
