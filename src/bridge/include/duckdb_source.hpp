#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "crossing.hpp"

namespace duckdb {

class ExtensionLoader;

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

struct BridgeContext {
	mutex lock;
	case_insensitive_map_t<Value> values;
	bool finalized = false;
};

//! The source catalog, held by identity: a DETACH, or another database attached under its name, does not
//! re-point the bridge.
class SourcePin {
public:
	SourcePin() = default;
	SourcePin(AttachedDatabase &attached, string registered_name);

	//! Binds the pinned database into the transaction of `source_context`, so every lookup of its name there
	//! resolves to it. Returns that name, which a rename may have changed. Throws CatalogException "was
	//! detached" when the pinned database is gone, its name now resolves to another database, or the
	//! transaction already references another database under that name.
	string Use(ClientContext &source_context) const;

private:
	weak_ptr<AttachedDatabase> attached;
	string registered_name;
};

struct GrantBook {
	mutex lock;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> tables;
	case_insensitive_set_t create_schemas;
	case_insensitive_map_t<shared_ptr<Connection>> shaping;
	shared_ptr<BridgeContext> context;
};

class DuckDBSession {
public:
	DuckDBSession(shared_ptr<DatabaseInstance> source_db, SourcePin source, shared_ptr<GrantBook> grants,
	              bool autocommit);

	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	void Commit();
	void Rollback();

private:
	shared_ptr<Connection> Shared();
	bool CreateWouldCreate(Connection &source_conn, const string &catalog, const CrossingDdl &ddl);
	void ThrowIfAlterOrphansPolicy(const CrossingDdl &ddl);
	void RecordDdl(const shared_ptr<Connection> &source_conn, const string &catalog, const CrossingDdl &ddl);
	void UndoGrants();
	void ForgetShaping();

	shared_ptr<DatabaseInstance> source_db;
	SourcePin source;
	shared_ptr<GrantBook> grants;
	bool autocommit;
	mutex lock;
	shared_ptr<Connection> conn;
	vector<std::function<void()>> grant_undo;
};

class DuckDBSource {
public:
	using Session = DuckDBSession;

	DuckDBSource(string bridge_id, const shared_ptr<DatabaseInstance> &source_db, DatabaseInstance &target_db,
	             SourcePin source, shared_ptr<GrantBook> grants);
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
	shared_ptr<DatabaseInstance> Database() const;

	bool SourceHasFunction(const string &function_name);

	string bridge_id;
	weak_ptr<DatabaseInstance> source_db;
	//! Null when source and target are one instance: the target's catalog would otherwise keep its own
	//! instance alive.
	shared_ptr<DatabaseInstance> source_db_hold;
	SourcePin source;
	shared_ptr<GrantBook> grants;
	mutex functions_lock;
	case_insensitive_map_t<bool> functions_known;
};

bool TryParseCrossingVerb(const string &text, CrossingVerb &out);

unique_ptr<DuckDBSource> RedeemBridgeAttach(AttachInfo &info, DatabaseInstance &target_db);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
