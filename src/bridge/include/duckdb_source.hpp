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

	SourceGrant() : using_predicates(CROSSING_VERB_COUNT), check_predicates(CROSSING_VERB_COUNT) {
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

class DuckDBSession : public CrossingSession {
public:
	DuckDBSession(shared_ptr<DatabaseInstance> source_db, string source_catalog, bool autocommit);

	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override;
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override;
	void Commit() override;
	void Rollback() override;

private:
	shared_ptr<Connection> Shared();

	shared_ptr<DatabaseInstance> source_db;
	string source_catalog;
	bool autocommit;
	mutex lock;
	shared_ptr<Connection> conn;
};

class DuckDBSource : public CrossingSource {
public:
	DuckDBSource(shared_ptr<DatabaseInstance> source_db, string source_catalog,
	             case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> granted);
	~DuckDBSource() override;

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingPlan Plan(const CrossingPlanRequest &request) override;
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	unique_ptr<CrossingSession> Begin(ClientContext &context) override;

private:
	shared_ptr<Connection> PlanningConnection();
	optional_ptr<const SourceGrant> GrantFor(const string &schema, const string &table) const;
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
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> granted;
	mutex functions_lock;
	case_insensitive_map_t<bool> functions_known;
};

bool TryParseCrossingVerb(const string &text, CrossingVerb &out);

unique_ptr<CrossingSource> RedeemBridgeAttach(ClientContext &context, AttachInfo &info);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
