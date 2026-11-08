#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "crossing.hpp"

namespace duckdb {

class CrossingAttach;
struct CrossingOpenTransaction;
class ExtensionLoader;

static constexpr const char *VCATBRIDGE_TYPE = "vcatbridge";

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
	shared_ptr<BridgeContext> context;
	//! The target's attach, refreshed when a grant changes after ATTACH. Null once the target catalog is gone.
	optional_ptr<CrossingAttach> target;
	//! Guarded by the bridge registry's mutex, not `lock`.
	bool policy_finalized = false;
	//! Grant changes from target DDL whose transaction is still open; their undo names tables, so the policy
	//! cannot be replaced under them.
	idx_t uncommitted_ddl = 0;
};

//! Grant changes from DDL a far transaction ran, undone unless the target's transaction commits.
class GrantLedger {
public:
	explicit GrantLedger(shared_ptr<GrantBook> grants);
	~GrantLedger();

	//! Caller holds the GrantBook's lock.
	void Push(std::function<void()> undo);
	void Resolve(bool committed);

private:
	//! Caller holds the GrantBook's lock.
	void Forget();

	shared_ptr<GrantBook> grants;
	vector<std::function<void()>> undo;
};

vector<shared_ptr<GrantLedger>> GrantLedgersOf(const CrossingOpenTransaction &transaction);

class DuckDBSource : public CrossingSource {
public:
	DuckDBSource(string bridge_id, const shared_ptr<DatabaseInstance> &source_db, DatabaseInstance &target_db,
	             SourcePin source, shared_ptr<GrantBook> grants);
	//! Unbound: every call throws until Bind. The target's main database starts this way, since it opens before
	//! any token can be redeemed.
	DuckDBSource();
	~DuckDBSource() override;

	//! Once, before any call can pass ThrowUnlessBound. Takes over `redeemed`, leaving it unbound.
	void Bind(DuckDBSource &redeemed);
	bool IsBound() const;

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingSchema DescribeSchema(const string &schema) override;
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	unique_ptr<CrossingLink> Connect(ClientContext &context) override;
	void DescribeChanged(const string &schema, CrossingTable &table) override;
	void SetTarget(optional_ptr<CrossingAttach> target);
	vector<string> GrantedKey(const string &schema, const string &name);
	// No Group(): PolicyResolver and GrantBook key on schema.table in this source's catalog, so a floor
	// naming another catalog would bind the wrong table under the wrong policy.

private:
	void ThrowUnlessBound() const;
	shared_ptr<Connection> PlanningConnection();
	vector<CrossingVerb> GrantedVerbs(const string &schema, const string &name);
	shared_ptr<DatabaseInstance> Database() const;

	//! Looks the function up where the source's plan deserializer will.
	bool SourceHasFunction(const BoundSimpleFunction &function, CatalogType type);

	string bridge_id;
	weak_ptr<DatabaseInstance> source_db;
	//! Null when source and target are one instance: the target's catalog would otherwise keep its own
	//! instance alive.
	shared_ptr<DatabaseInstance> source_db_hold;
	SourcePin source;
	shared_ptr<GrantBook> grants;
	atomic<bool> bound;
};

unique_ptr<DuckDBSource> RedeemBridge(const string &bridge_id, const string &token, DatabaseInstance &target_db);
unique_ptr<DuckDBSource> RedeemBridgeAttach(AttachInfo &info, DatabaseInstance &target_db);

void RegisterBridgeFunctions(ExtensionLoader &loader);

} // namespace duckdb
