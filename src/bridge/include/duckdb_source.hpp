#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

class ExtensionLoader;
class SQLStatement;
class TableRef;

static constexpr const char *VIRTUAL_CATALOG_BRIDGE_TYPE = "virtual_catalog_bridge";

struct SourceGrant {
	uint8_t verbs = 0;
	vector<unique_ptr<ParsedExpression>> using_predicates;
	vector<unique_ptr<ParsedExpression>> check_predicates;
	vector<string> key;
	bool key_verified = true;

	SourceGrant() : using_predicates(CrossingVerbs().size()), check_predicates(CrossingVerbs().size()) {
	}

	void Allow(CrossingVerb verb) {
		verbs |= static_cast<uint8_t>(1u << static_cast<uint8_t>(verb));
	}
	bool Has(CrossingVerb verb) const {
		return (verbs & static_cast<uint8_t>(1u << static_cast<uint8_t>(verb))) != 0;
	}
	unique_ptr<ParsedExpression> CopyUsing(CrossingVerb verb) const {
		auto &predicate = using_predicates[static_cast<idx_t>(verb)];
		return predicate ? predicate->Copy() : nullptr;
	}
	unique_ptr<ParsedExpression> CopyCheck(CrossingVerb verb) const {
		auto &predicate = check_predicates[static_cast<idx_t>(verb)];
		return predicate ? predicate->Copy() : nullptr;
	}
};

struct GrantBook {
	mutex lock;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> tables;
	case_insensitive_set_t create_schemas;
	case_insensitive_map_t<shared_ptr<Connection>> shaping;
};

class DuckDBSession {
public:
	DuckDBSession(shared_ptr<DatabaseInstance> source_db, string source_catalog, shared_ptr<GrantBook> grants,
	              bool autocommit);

	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	void Commit();
	void Rollback();

private:
	shared_ptr<Connection> Shared();
	void RecordDdl(const shared_ptr<Connection> &source_conn, const CrossingDdl &ddl);
	void ForgetShaping();

	shared_ptr<DatabaseInstance> source_db;
	string source_catalog;
	shared_ptr<GrantBook> grants;
	bool autocommit;
	mutex lock;
	shared_ptr<Connection> conn;
	vector<std::function<void()>> grant_undo;
};

class DuckDBSource {
public:
	using Session = DuckDBSession;

	DuckDBSource(shared_ptr<DatabaseInstance> source_db, string source_catalog, shared_ptr<GrantBook> grants);
	~DuckDBSource();

	vector<string> Schemas();
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	CrossingSchema DescribeSchema(const string &schema);
	CrossingPlan Plan(const CrossingPlanRequest &request);
	CrossingVerdict AcceptsCall(const Expression &expr);
	CrossingVerdict AcceptsType(const LogicalType &type);
	unique_ptr<DuckDBSession> Begin(ClientContext &context);

private:
	shared_ptr<Connection> PlanningConnection();
	shared_ptr<Connection> PlanningConnection(const string &schema, const string &table);
	unique_ptr<ParsedExpression> UsingFor(const string &schema, const string &table, CrossingVerb verb);
	unique_ptr<ParsedExpression> CheckFor(const string &schema, const string &table, CrossingVerb verb);
	unique_ptr<LogicalOperator> ScanPlan(const CrossingPlanRequest &request);
	unique_ptr<SQLStatement> InsertStatement(const CrossingPlanRequest &request, const vector<string> &row_aliases,
	                                         unique_ptr<TableRef> rows_ref);
	unique_ptr<SQLStatement> DeleteStatement(const CrossingPlanRequest &request, const vector<string> &row_aliases,
	                                         unique_ptr<TableRef> rows_ref);
	unique_ptr<SQLStatement> UpdateStatement(const CrossingPlanRequest &request, const vector<string> &row_aliases,
	                                         unique_ptr<TableRef> rows_ref);

	bool SourceHasFunction(const string &function_name);

	shared_ptr<DatabaseInstance> source_db;
	string source_catalog;
	shared_ptr<GrantBook> grants;
	mutex functions_lock;
	case_insensitive_map_t<bool> functions_known;
};

bool TryParseCrossingVerb(const string &text, CrossingVerb &out);

unique_ptr<DuckDBSource> RedeemBridgeAttach(ClientContext &context, AttachInfo &info);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
