#include "catch.hpp"
#include "policy_config.hpp"

using namespace duckdb;

namespace {

string ErrorOf(const std::function<void()> &f) {
	try {
		f();
	} catch (std::exception &ex) {
		return ErrorData(ex).RawMessage();
	}
	return "";
}

const PolicyVerb &VerbOf(const PolicyTable &table, CrossingVerb verb) {
	return table.verbs[static_cast<idx_t>(verb)];
}

} // namespace

TEST_CASE("policy_config: names parse as SQL identifiers", "[policy_config]") {
	CHECK(ParsePolicyNameParts("main.users") == vector<string> {"main", "users"});
	CHECK(ParsePolicyNameParts("\"my.schema\".t") == vector<string> {"my.schema", "t"});
	CHECK(ParsePolicyNameParts("\"say \"\"hi\"\"\".t") == vector<string> {"say \"hi\"", "t"});
	CHECK(ParsePolicyNameParts("\" padded \".t") == vector<string> {" padded ", "t"});

	auto three = ParsePolicyTableName("app.main.users");
	CHECK(three.catalog == "app");
	CHECK(three.schema == "main");
	CHECK(three.name == "users");

	auto schema = ParsePolicySchemaName("main");
	CHECK(schema.catalog.empty());
	CHECK(schema.schema == "main");

	for (auto bad :
	     {"", "main.", ".users", "main..users", " main.users", "main.users ", "\"open.users", "a\"b\".c", "\"a\"b.c"}) {
		INFO(bad);
		CHECK_FALSE(ErrorOf([&]() { ParsePolicyNameParts(bad); }).empty());
	}
	CHECK_THAT(ErrorOf([]() { ParsePolicyTableName("users"); }), Catch::Contains("'schema.table'"));
	CHECK_THAT(ErrorOf([]() { ParsePolicyTableName("a.b.c.d"); }), Catch::Contains("'schema.table'"));
}

TEST_CASE("policy_config: a whole document parses", "[policy_config]") {
	auto config = ParsePolicyJson(R"({
		"$schema": "policy-v1.schema.json",
		"policy_version": 1,
		"schemas": {"main": {"create": true}, "scratch": {"create": false}},
		"tables": {
			"main.users": {
				"primary_key": ["id"],
				"select": "tenant = $tenant",
				"insert": "tenant = $tenant",
				"update": {"check": "tenant = 1"},
				"delete": true,
				"drop": true,
				"foreign_keys": ["main.orgs"]
			},
			"app.main.legacy": {
				"primary_key": {"columns_from_query": "SELECT 'id'", "verify_unique": "SELECT 1 WHERE false"},
				"select": true
			}
		},
		"functions": {"add": ["my_udf"], "remove": ["random"]}
	})");
	REQUIRE(config.create_schemas.size() == 1);
	CHECK(config.create_schemas[0].name.schema == "main");
	REQUIRE(config.tables.size() == 2);

	auto &users = config.tables[0];
	CHECK(users.path == "tables.\"main.users\"");
	CHECK(users.has_key);
	CHECK(users.key.columns == vector<string> {"id"});
	CHECK(VerbOf(users, CrossingVerb::SELECT).using_text == "tenant = $tenant");
	CHECK(VerbOf(users, CrossingVerb::INSERT).using_text == "true");
	CHECK(VerbOf(users, CrossingVerb::INSERT).check_text == "tenant = $tenant");
	CHECK(VerbOf(users, CrossingVerb::UPDATE).using_text == "true");
	CHECK(VerbOf(users, CrossingVerb::UPDATE).check_text == "tenant = 1");
	CHECK(VerbOf(users, CrossingVerb::DELETE_).using_text == "true");
	CHECK(VerbOf(users, CrossingVerb::DROP).granted);
	CHECK_FALSE(VerbOf(users, CrossingVerb::ALTER).granted);
	REQUIRE(users.foreign_keys.size() == 1);
	CHECK(users.foreign_keys[0].name == "orgs");

	auto &legacy = config.tables[1];
	CHECK(legacy.name.catalog == "app");
	CHECK(legacy.key.columns_from_query == "SELECT 'id'");
	CHECK(legacy.key.verify_unique == "SELECT 1 WHERE false");

	CHECK(config.functions.builtins);
	CHECK(config.functions.add == vector<string> {"my_udf"});
	CHECK(config.functions.remove == vector<string> {"random"});
}

TEST_CASE("policy_config: shorthands", "[policy_config]") {
	auto config = ParsePolicyJson(R"({"policy_version": 1, "tables": {"main.t": {
		"insert": true, "update": "a = 1"}}, "functions": ["abs"]})");
	auto &t = config.tables[0];
	CHECK(VerbOf(t, CrossingVerb::INSERT).check_text == "true");
	CHECK(VerbOf(t, CrossingVerb::UPDATE).using_text == "a = 1");
	CHECK(VerbOf(t, CrossingVerb::UPDATE).check_text.empty());
	CHECK_FALSE(config.functions.builtins);
	CHECK(config.functions.add == vector<string> {"abs"});
}

TEST_CASE("policy_config: errors name where they are", "[policy_config]") {
	const auto error = [](const string &json) {
		return ErrorOf([&]() { ParsePolicyJson(json); });
	};
	CHECK_THAT(error("{"), Catch::Contains("not valid JSON"));
	CHECK_THAT(error("[]"), Catch::Contains("must be a JSON object"));
	CHECK_THAT(error(R"({"tables": {}})"), Catch::Contains("policy_version is required"));
	CHECK_THAT(error(R"({"policy_version": "1"})"), Catch::Contains("supports policy_version 1"));
	CHECK_THAT(error(R"({"policy_version": 2})"), Catch::Contains("supports policy_version 1"));
	CHECK_THAT(error(R"({"policy_version": 1, "table": {}})"), Catch::Contains("unknown key \"table\""));
	CHECK_THAT(error(R"({"policy_version": 1, "policy_version": 1})"), Catch::Contains("appears twice"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": true, "selct": true}}})"),
	           Catch::Contains("tables.\"main.t\": unknown key \"selct\""));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"primary_key": ["id"]}}})"),
	           Catch::Contains("tables.\"main.t\": grants no verb"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": null}}})"),
	           Catch::Contains("tables.\"main.t\".select: null"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": ""}}})"),
	           Catch::Contains("cannot be empty"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"drop": "a = 1"}}})"),
	           Catch::Contains("'drop' takes only true"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"insert": {"using": "a = 1"}}}})"),
	           Catch::Contains("unknown key \"using\""));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"update": {"check": "a", "chek": "b"}}}})"),
	           Catch::Contains("tables.\"main.t\".update: unknown key \"chek\""));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": true, "primary_key": []}}})"),
	           Catch::Contains("cannot be empty"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": true,
		"primary_key": {"columns": ["id"], "columns_from_query": "SELECT 1"}}}})"),
	           Catch::Contains("exactly one of columns or columns_from_query"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"main.t": {"select": true, "foreign_keys": ["other.p"]}}})"),
	           Catch::Contains("never crosses schemas"));
	CHECK_THAT(error(R"({"policy_version": 1, "tables": {"t": {"select": true}}})"), Catch::Contains("tables.t: "));
	CHECK_THAT(error(R"({"policy_version": 1, "functions": {"allow": ["f"]}})"),
	           Catch::Contains("functions: unknown key \"allow\""));
}

TEST_CASE("policy_config: setter values", "[policy_config]") {
	string add = "add";
	string set = "set";

	auto select = ParsePolicyEdit("main.t", "select", nullptr, Value("a = 1"));
	CHECK(select.kind == PolicyEditKind::VERB);
	CHECK(select.verb_value.using_text == "a = 1");

	CHECK(ParsePolicyEdit("main.t", "delete", nullptr, Value::BOOLEAN(true)).verb_value.granted);
	CHECK_FALSE(ParsePolicyEdit("main.t", "select", nullptr, Value::BOOLEAN(false)).verb_value.granted);
	CHECK_FALSE(ParsePolicyEdit("main.t", "select", nullptr, Value()).verb_value.granted);

	child_list_t<Value> fields;
	fields.emplace_back("using", Value("a = 1"));
	fields.emplace_back("check", Value("a = 2"));
	auto update = ParsePolicyEdit("main.t", "update", nullptr, Value::STRUCT(std::move(fields)));
	CHECK(update.verb_value.using_text == "a = 1");
	CHECK(update.verb_value.check_text == "a = 2");

	auto key = ParsePolicyEdit("main.t", "primary_key", nullptr, Value::LIST({Value("id")}));
	CHECK(key.has_key);
	CHECK(key.key.columns == vector<string> {"id"});
	CHECK_FALSE(ParsePolicyEdit("main.t", "primary_key", nullptr, Value()).has_key);

	auto create = ParsePolicyEdit("main", "create", nullptr, Value::BOOLEAN(true));
	CHECK(create.kind == PolicyEditKind::CREATE);
	CHECK(create.target.schema == "main");

	auto fk = ParsePolicyEdit("main.t", "foreign_keys", &add, Value("main.p"));
	CHECK(fk.op == PolicyListOp::ADD);
	CHECK(fk.foreign_keys[0].name == "p");

	auto whitelist = ParsePolicyEdit(":functions", "whitelist", &set, Value::LIST({Value("abs")}));
	CHECK(whitelist.kind == PolicyEditKind::WHITELIST);
	CHECK(whitelist.functions == vector<string> {"abs"});
	auto cleared = ParsePolicyEdit(":functions", "whitelist", &set, Value::LIST(LogicalType::VARCHAR, {}));
	CHECK(cleared.functions.empty());
}

TEST_CASE("policy_config: setter errors", "[policy_config]") {
	string add = "add";
	string bogus = "append";
	const auto error = [](const string &target, const string &setting, const string *op, const Value &value) {
		return ErrorOf([&]() { ParsePolicyEdit(target, setting, op, value); });
	};
	CHECK_THAT(error("main.t", "foreign_keys", nullptr, Value("main.p")), Catch::Contains("pass an operation"));
	CHECK_THAT(error("main.t", "select", &add, Value("true")), Catch::Contains("without an operation"));
	CHECK_THAT(error("main.t", "foreign_keys", &bogus, Value("main.p")), Catch::Contains("unknown operation"));
	CHECK_THAT(error("main.t", "selct", nullptr, Value("true")), Catch::Contains("unknown setting"));
	CHECK_THAT(error(":function", "whitelist", &add, Value("f")), Catch::Contains("unknown target"));
	CHECK_THAT(error("main.t", "select", nullptr, Value::INTEGER(1)), Catch::Contains("not INTEGER"));
	CHECK_THAT(error("main.t", "foreign_keys", &add, Value()), Catch::Contains("cannot be NULL"));

	child_list_t<Value> miscased;
	miscased.emplace_back("USING", Value("a = 1"));
	CHECK_THAT(error("main.t", "update", nullptr, Value::STRUCT(std::move(miscased))),
	           Catch::Contains("unknown key \"USING\""));
	CHECK_THAT(error("main.t", "primary_key", nullptr, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, {}, {})),
	           Catch::Contains("expected a string, boolean, list or struct"));
}
