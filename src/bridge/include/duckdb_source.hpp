#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "crossing.hpp"

namespace duckdb {

class CrossingAttach;
class ExtensionLoader;

static constexpr const char *VIRTUAL_CATALOG_BRIDGE_TYPE = "virtual_catalog_bridge";

struct SourceGrant {
	uint8_t verbs = 0;
	vector<unique_ptr<ParsedExpression>> using_predicates;
	vector<unique_ptr<ParsedExpression>> check_predicates;
	vector<string> key;
	//! The table's own PRIMARY KEY, which `key` falls back to when no key is set.
	vector<string> discovered_key;
	bool key_set = false;
	bool key_verified = true;
	bool select_unrestricted = false;
	//! Same schema as the grant: DuckDB refuses a foreign key across schemas.
	vector<string> foreign_key_parents;
	case_insensitive_set_t allowed_foreign_keys;

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
	SourceGrant Copy() const {
		SourceGrant copy;
		copy.verbs = verbs;
		for (auto verb : CrossingVerbs()) {
			copy.using_predicates[static_cast<idx_t>(verb)] = CopyUsing(verb);
			copy.check_predicates[static_cast<idx_t>(verb)] = CopyCheck(verb);
		}
		copy.key = key;
		copy.discovered_key = discovered_key;
		copy.key_set = key_set;
		copy.key_verified = key_verified;
		copy.select_unrestricted = select_unrestricted;
		copy.foreign_key_parents = foreign_key_parents;
		copy.allowed_foreign_keys = allowed_foreign_keys;
		return copy;
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
	//! resolves to it. Returns that name, which a rename may have changed. Throws CatalogException when the
	//! pinned database is no longer attached under its name, or the transaction already references another
	//! database under that name.
	string Use(ClientContext &source_context) const;

private:
	weak_ptr<AttachedDatabase> attached;
	string registered_name;
};

//! The owner's changes to which calls a bridge pushes to the source; `PushableBuiltins` is the default.
struct FunctionGrants {
	//! False once the owner set the whitelist exactly.
	bool builtins = true;
	case_insensitive_set_t allowed;
	case_insensitive_set_t disallowed;
};

struct GrantBook {
	mutex lock;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> tables;
	case_insensitive_set_t create_schemas;
	FunctionGrants functions;
	case_insensitive_map_t<shared_ptr<Connection>> shaping;
	shared_ptr<BridgeContext> context;
	//! The target's attach, refreshed when a grant changes after ATTACH. Null once the target catalog is gone.
	optional_ptr<CrossingAttach> target;
	//! Guarded by the bridge registry's mutex, not `lock`.
	bool policy_finalized = false;
	//! Grant changes from target DDL whose transaction is still open; their undo names tables, so the policy
	//! cannot be replaced under them.
	idx_t uncommitted_ddl = 0;
};

class DuckDBSession {
public:
	DuckDBSession(shared_ptr<DatabaseInstance> source_db, SourcePin source, shared_ptr<GrantBook> grants,
	              bool autocommit);
	~DuckDBSession();

	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	void Commit();
	void Rollback();

private:
	shared_ptr<Connection> Shared();
	bool CreateWouldCreate(Connection &source_conn, const string &catalog, const CrossingDdl &ddl);
	//! Throws unless both the alter grant and the select policy fold to true under the current context: an ALTER
	//! runs over every row on the source, including rows the select policy would hide from this target.
	void ThrowUnlessAlterGrantedOverFullTable(ClientContext &source_context, const string &catalog,
	                                          const CrossingDdl &ddl);
	void ThrowIfAlterOrphansPolicy(const CrossingDdl &ddl);
	//! The source checks a foreign key over every referenced row, hidden ones included.
	void ThrowIfCreateDeclaresForeignKey(const CrossingDdl &ddl);
	//! Non-constant defaults, CHECKs and generated columns are stored on the source and run on the owner's later
	//! inserts; an ALTER COLUMN TYPE ... USING cast runs on the source at once. Each would run over rows the
	//! target cannot see.
	void ThrowIfDdlCarriesExpression(const CrossingDdl &ddl);
	void RecordDdl(const shared_ptr<Connection> &source_conn, const string &catalog, const CrossingDdl &ddl);
	//! Caller holds the GrantBook's lock.
	void PushGrantUndo(std::function<void()> undo);
	void ForgetGrantUndo();
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
	void SetTarget(optional_ptr<CrossingAttach> target);
	// No Group(): PolicyResolver and GrantBook key on schema.table in this source's catalog, so a floor
	// naming another catalog would bind the wrong table under the wrong policy.

private:
	shared_ptr<Connection> PlanningConnection();
	shared_ptr<DatabaseInstance> Database() const;

	//! Looks the function up where the source's plan deserializer will.
	bool SourceHasFunction(const Function &function, CatalogType type);

	string bridge_id;
	weak_ptr<DatabaseInstance> source_db;
	//! Null when source and target are one instance: the target's catalog would otherwise keep its own
	//! instance alive.
	shared_ptr<DatabaseInstance> source_db_hold;
	SourcePin source;
	shared_ptr<GrantBook> grants;
};

unique_ptr<DuckDBSource> RedeemBridgeAttach(AttachInfo &info, DatabaseInstance &target_db);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
