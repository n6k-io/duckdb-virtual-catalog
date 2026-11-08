#include "duckdb_source.hpp"

#include "builtin_functions.hpp"
#include "crossing_attach.hpp"
#include "crossing_far.hpp"
#include "crossing_in_process.hpp"
#include "policy_config.hpp"
#include "predicate_fold.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

namespace duckdb {

namespace {

struct PendingSource {
	//! Weak: a registration nothing attached to must not keep a closed source's instance and its threads alive.
	weak_ptr<DatabaseInstance> source_db;
	string token;
	SourcePin source;
	std::chrono::steady_clock::time_point last_touched;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> schemas;
	case_insensitive_set_t create_schemas;
	FunctionGrants functions;
	shared_ptr<BridgeContext> context = make_shared_ptr<BridgeContext>();
	bool policy_finalized = false;
	idx_t token_failures = 0;
};

struct LiveBridge {
	DatabaseInstance *source_db;
	SourcePin source;
	weak_ptr<GrantBook> grants;
};

constexpr int64_t PENDING_SOURCE_TTL_SECONDS = 30;
//! A pending entry is dropped on this many wrong tokens.
constexpr idx_t MAX_TOKEN_FAILURES = 5;

const string &LoadNonce() {
	static const string nonce = UUID::ToString(UUID::GenerateRandomUUID());
	return nonce;
}

//! Never held while a source transaction ends: that transaction can hold the last reference to a detached
//! bridge, whose ~DuckDBSource takes this mutex.
mutex &PendingMutex() {
	static auto *lock = new mutex();
	return *lock;
}

unordered_map<string, PendingSource> &PendingSources() {
	static auto *pending = new unordered_map<string, PendingSource>();
	return *pending;
}

unordered_map<string, LiveBridge> &LiveBridges() {
	static auto *live = new unordered_map<string, LiveBridge>();
	return *live;
}

//! A reference locked from a pending entry may be the last one to a DatabaseInstance whose ~DuckDBSource takes
//! PendingMutex, so it parks in `held`, declared before `guard` so it is destroyed after the unlock.
struct PendingLock {
	vector<shared_ptr<DatabaseInstance>> held;
	lock_guard<mutex> guard {PendingMutex()};

	shared_ptr<DatabaseInstance> Pin(const PendingSource &pending) {
		auto db = pending.source_db.lock();
		if (db) {
			held.push_back(db);
		}
		return db;
	}

	void Drop(unordered_map<string, PendingSource>::iterator it) {
		PendingSources().erase(it);
	}
};

//! Caller holds PendingMutex.
bool IsLiveBridge(const string &bridge_id) {
	auto it = LiveBridges().find(bridge_id);
	return it != LiveBridges().end() && !it->second.grants.expired();
}

bool ConstantTimeEquals(const string &a, const string &b) {
	if (a.size() != b.size()) {
		return false;
	}
	unsigned char diff = 0;
	for (idx_t i = 0; i < a.size(); i++) {
		diff |= static_cast<unsigned char>(a[i] ^ b[i]);
	}
	return diff == 0;
}

void PurgeExpiredPendingSources(PendingLock &lock) {
	auto now = std::chrono::steady_clock::now();
	auto &pending = PendingSources();
	for (auto it = pending.begin(); it != pending.end();) {
		auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_touched).count();
		auto next = std::next(it);
		if (idle > PENDING_SOURCE_TTL_SECONDS || it->second.source_db.expired()) {
			lock.Drop(it);
		}
		it = next;
	}
}

PendingSource &LookupPendingForGrant(PendingLock &lock, const string &bridge_id, ClientContext &context) {
	auto it = PendingSources().find(bridge_id);
	if (it == PendingSources().end()) {
		throw PermissionException("vcatbridge: no registered source for bridge '%s'. "
		                          "Call vcatbridge_register_source on the source connection first.",
		                          bridge_id);
	}
	if (lock.Pin(it->second).get() != context.db.get()) {
		throw PermissionException("vcatbridge: bridge '%s' was registered by a different source connection", bridge_id);
	}
	it->second.last_touched = std::chrono::steady_clock::now();
	return it->second;
}

//! Caller holds PendingMutex.
shared_ptr<GrantBook> LookupLive(const string &bridge_id, ClientContext &context, SourcePin &source) {
	auto it = LiveBridges().find(bridge_id);
	shared_ptr<GrantBook> grants;
	if (it != LiveBridges().end()) {
		grants = it->second.grants.lock();
	}
	if (!grants) {
		throw PermissionException("vcatbridge: no registered source for bridge '%s'", bridge_id);
	}
	if (it->second.source_db != context.db.get()) {
		throw PermissionException("vcatbridge: bridge '%s' was registered by a different source connection", bridge_id);
	}
	source = it->second.source;
	return grants;
}

using GrantTables = case_insensitive_map_t<case_insensitive_map_t<SourceGrant>>;

//! A bridge's grants: the pending entry's before ATTACH, the live GrantBook's after. Valid while the caller
//! holds PendingMutex.
struct GrantTarget {
	shared_ptr<DatabaseInstance> source_db;
	SourcePin source;
	shared_ptr<BridgeContext> context;
	optional_ptr<PendingSource> pending;
	shared_ptr<GrantBook> live;
	bool *policy_finalized = nullptr;
};

GrantTarget LookupGrantTarget(PendingLock &lock, const string &bridge_id, ClientContext &context) {
	GrantTarget target;
	PurgeExpiredPendingSources(lock);
	if (PendingSources().count(bridge_id)) {
		auto &pending = LookupPendingForGrant(lock, bridge_id, context);
		target.source_db = lock.Pin(pending);
		target.source = pending.source;
		target.context = pending.context;
		target.pending = &pending;
		target.policy_finalized = &pending.policy_finalized;
		return target;
	}
	target.live = LookupLive(bridge_id, context, target.source);
	target.source_db = context.db;
	target.context = target.live->context;
	target.policy_finalized = &target.live->policy_finalized;
	return target;
}

GrantTarget LookupOpenGrants(PendingLock &lock, const string &bridge_id, ClientContext &context) {
	auto target = LookupGrantTarget(lock, bridge_id, context);
	if (*target.policy_finalized) {
		throw PermissionException("vcatbridge: the policies of bridge '%s' are finalized", bridge_id);
	}
	return target;
}

optional_ptr<SourceGrant> FindGrant(GrantTables &tables, const string &schema, const string &table) {
	auto schema_it = tables.find(schema);
	if (schema_it == tables.end()) {
		return nullptr;
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? nullptr : &table_it->second;
}

SourcePin PinSourceCatalog(ClientContext &context, const string &source_catalog) {
	auto entry = Catalog::GetCatalogEntry(context, Identifier(source_catalog));
	if (!entry) {
		throw CatalogException("vcatbridge: source catalog '%s' does not exist", source_catalog);
	}
	if (entry->IsTemporaryCatalog() || entry->IsSystemCatalog()) {
		throw CatalogException("vcatbridge: source catalog '%s' is a temporary or system catalog", source_catalog);
	}
	return SourcePin(entry->GetAttached(), source_catalog);
}

void ThrowIfAnyNull(DataChunk &args, idx_t columns, const char *function) {
	for (idx_t c = 0; c < columns; c++) {
		auto &validity = FlatVector::Validity(args.data[c]);
		for (idx_t i = 0; i < args.size(); i++) {
			if (!validity.RowIsValid(i)) {
				throw IOException("vcatbridge: argument %llu of %s cannot be NULL", static_cast<uint64_t>(c + 1),
				                  function);
			}
		}
	}
}

void RegisterSourceFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto arg_count = args.ColumnCount();
	for (idx_t c = 0; c < arg_count; c++) {
		args.data[c].Flatten();
	}
	ThrowIfAnyNull(args, arg_count, "vcatbridge_register_source");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto source_catalogs = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto source_catalog = source_catalogs[i].GetString();
		if (bridge_id.empty()) {
			throw IOException("vcatbridge: bridge id cannot be empty");
		}
		PendingSource pending;
		pending.source = PinSourceCatalog(context, source_catalog);
		pending.source_db = context.db;
		if (arg_count > 2) {
			auto chosen = FlatVector::GetData<string_t>(args.data[2])[i].GetString();
			if (chosen.empty()) {
				throw IOException("vcatbridge: the token cannot be empty");
			}
			pending.token = std::move(chosen);
		} else {
			pending.token = LoadNonce() + ":" + UUID::ToString(UUID::GenerateRandomUUID());
		}
		pending.last_touched = std::chrono::steady_clock::now();

		PendingLock lock;
		PurgeExpiredPendingSources(lock);
		if (PendingSources().find(bridge_id) != PendingSources().end() || IsLiveBridge(bridge_id)) {
			throw IOException("vcatbridge: a source is already registered for bridge '%s'", bridge_id);
		}
		result_data[i] = StringVector::AddString(result, pending.token);
		PendingSources()[bridge_id] = std::move(pending);
	}
}

bool IsGrantable(optional_ptr<CatalogEntry> entry) {
	return entry && (entry->type == CatalogType::TABLE_ENTRY || entry->type == CatalogType::VIEW_ENTRY);
}

//! Connection::View drops the catalog, so a view is named through a BaseTableRef instead.
shared_ptr<Relation> SourceRelation(Connection &conn, const CatalogEntry &entry, const string &catalog,
                                    const string &schema, const string &name) {
	if (entry.type == CatalogType::TABLE_ENTRY) {
		return conn.Table(Identifier(catalog), Identifier(schema), Identifier(name));
	}
	auto ref = make_uniq<BaseTableRef>();
	ref->SetQualifiedName(Identifier(catalog), Identifier(schema), Identifier(name));
	return make_shared_ptr<ViewRelation>(conn.context, std::move(ref), name);
}

void ClearQueryLocations(ParsedExpression &expr) {
	expr.SetQueryLocation(optional_idx());
	ParsedExpressionIterator::EnumerateChildren(expr, [](ParsedExpression &child) { ClearQueryLocations(child); });
}

bool IsUnrestricted(const string &text) {
	auto first = text.find_first_not_of(" \t\n\r");
	if (first == string::npos) {
		return false;
	}
	auto last = text.find_last_not_of(" \t\n\r");
	return StringUtil::CIEquals(text.substr(first, last - first + 1), "true");
}

bool MentionsColumn(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { found = found || MentionsColumn(child); });
	return found;
}

void ContextNames(const ParsedExpression &expr, case_insensitive_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::PARAMETER) {
		names.insert(expr.Cast<ParameterExpression>().Identifier().GetIdentifierName());
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](const ParsedExpression &child) { ContextNames(child, names); });
}

void FillContext(unique_ptr<ParsedExpression> &expr, const case_insensitive_map_t<Value> &values) {
	if (expr->GetExpressionClass() == ExpressionClass::PARAMETER) {
		auto &name = expr->Cast<ParameterExpression>().Identifier().GetIdentifierName();
		auto it = values.find(name);
		if (it == values.end()) {
			throw InternalException("vcatbridge: context '$%s' is not set", name);
		}
		expr = ConstantExpression::FromValue(it->second);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { FillContext(child, values); });
}

case_insensitive_map_t<Value> ContextSnapshot(BridgeContext &context) {
	lock_guard<mutex> guard(context.lock);
	return context.values;
}

void ProbePredicate(const shared_ptr<Relation> &source_table, const ParsedExpression &predicate,
                    const case_insensitive_map_t<Value> &values, const string &text, const char *clause) {
	auto expr = predicate.Copy();
	FillContext(expr, values);
	shared_ptr<Relation> probe;
	try {
		source_table->Filter(expr->Copy());
		vector<unique_ptr<ParsedExpression>> projection;
		projection.push_back(expr->Copy());
		probe = source_table->Project(std::move(projection), vector<string> {"policy"});
	} catch (const std::exception &ex) {
		throw IOException("vcatbridge: the %s predicate '%s' is not a valid WHERE clause on the "
		                  "source: %s",
		                  clause, text, ex.what());
	}
	auto &columns = probe->Columns();
	if (columns.size() != 1) {
		throw IOException("vcatbridge: the %s predicate '%s' must be a single expression", clause, text);
	}
	if (columns[0].Type().id() != LogicalTypeId::BOOLEAN) {
		throw IOException("vcatbridge: the %s predicate '%s' must be BOOLEAN, but it is %s", clause, text,
		                  columns[0].Type().ToString());
	}
}

//! Without `values`, a predicate that uses context is parsed but not probed: binding needs the values' types.
unique_ptr<ParsedExpression> ParseAndValidatePredicate(const shared_ptr<Relation> &source_table, const string &text,
                                                       const char *clause,
                                                       optional_ptr<const case_insensitive_map_t<Value>> values) {
	auto parsed = Parser::GetBuiltinParser().ParseExpressionList(text);
	if (parsed.size() != 1) {
		throw IOException("vcatbridge: the %s predicate must be a single expression", clause);
	}
	auto expr = std::move(parsed[0]);
	case_insensitive_set_t names;
	ContextNames(*expr, names);
	for (auto &name : names) {
		if (std::all_of(name.begin(), name.end(), StringUtil::CharacterIsDigit)) {
			throw IOException("vcatbridge: the %s predicate '%s' may only use named context "
			                  "parameters ($name), not positional ones",
			                  clause, text);
		}
		if (values && !values->count(name)) {
			throw IOException("vcatbridge: the %s predicate '%s' uses context '$%s', which is not set; "
			                  "call vcatbridge_set_context before setting a policy that uses it",
			                  clause, text, name);
		}
	}
	ClearQueryLocations(*expr);
	if (values || names.empty()) {
		ProbePredicate(source_table, *expr, values ? *values : case_insensitive_map_t<Value>(), text, clause);
	}
	return expr;
}

vector<string> DiscoverKeyColumns(TableCatalogEntry &table) {
	vector<string> key;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		if (unique.HasIndex()) {
			key.push_back(table.GetColumns().GetColumn(unique.GetIndex()).GetName().GetIdentifierName());
		} else {
			for (auto &column : unique.GetColumnNames()) {
				key.push_back(column.GetIdentifierName());
			}
		}
		break;
	}
	return key;
}

vector<string> DiscoverForeignKeyParents(TableCatalogEntry &table) {
	vector<string> parents;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::FOREIGN_KEY) {
			continue;
		}
		auto &info = constraint->Cast<ForeignKeyConstraint>().info;
		if (info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
			parents.push_back(info.table.GetIdentifierName());
		} else if (info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
			parents.push_back(table.name.GetIdentifierName());
		}
	}
	return parents;
}

//! The source checks a foreign key over every parent row, hidden ones included, so an insert or update reveals
//! whether a parent key exists unless the target can already read every parent row.
void ThrowIfForeignKeyOracle(GrantTables &tables, const string &schema, const string &table,
                             const vector<string> &parents, const case_insensitive_set_t &allowed, CrossingVerb verb,
                             const string &bridge_id) {
	if (verb != CrossingVerb::INSERT && verb != CrossingVerb::UPDATE) {
		return;
	}
	for (auto &parent : parents) {
		if (allowed.count(parent)) {
			continue;
		}
		auto parent_grant = FindGrant(tables, schema, parent);
		if (parent_grant && parent_grant->Has(CrossingVerb::SELECT) && parent_grant->select_unrestricted) {
			continue;
		}
		throw PermissionException(
		    "vcatbridge: table '%s.%s' in bridge '%s' grants '%s' over a foreign key to '%s.%s', which "
		    "is not granted 'select' with USING 'true'; the write would reveal which keys exist in it. To accept "
		    "this, add '%s.%s' to the foreign_keys of '%s.%s'",
		    schema, table, bridge_id, CrossingVerbName(verb), schema, parent, schema, parent, schema, table);
	}
}

optional_ptr<CatalogEntry> SourceTableEntry(ClientContext &context, const string &catalog, const string &schema,
                                            const string &name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       QualifiedName(Identifier(catalog), Identifier(schema), Identifier(name)));
	return Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
}

//! Rewrites every unqualified column reference in `expr`, the root included: a written column becomes the value
//! written to it, anything else is pinned to `alias`. Without an alias there is no row to read it from.
void PointAtWrittenRow(unique_ptr<ParsedExpression> &expr,
                       const case_insensitive_map_t<const ParsedExpression *> &written,
                       optional_ptr<const Identifier> alias, const string &table) {
	if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_ref = expr->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			return;
		}
		auto name = column_ref.GetColumnName();
		auto it = written.find(name.GetIdentifierName());
		if (it != written.end()) {
			expr = it->second->Copy();
			return;
		}
		if (!alias) {
			throw PermissionException("vcatbridge: the check predicate on '%s' reads '%s', which the insert does not "
			                          "write",
			                          table, name.GetIdentifierName());
		}
		expr = make_uniq<ColumnRefExpression>(name, *alias);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { PointAtWrittenRow(child, written, alias, table); });
}

void QualifyColumns(unique_ptr<ParsedExpression> &expr, const Identifier &alias, const string &table) {
	PointAtWrittenRow(expr, {}, &alias, table);
}

unique_ptr<ParsedExpression> GuardWithCheck(unique_ptr<ParsedExpression> check, unique_ptr<ParsedExpression> value) {
	vector<unique_ptr<ParsedExpression>> error_args;
	error_args.push_back(ConstantExpression::String("vcatbridge: row violates the check predicate on this table"));
	auto guard = make_uniq<CaseExpression>();
	CaseCheck branch;
	branch.when_expr = std::move(check);
	branch.then_expr = std::move(value);
	guard->CaseChecksMutable().push_back(std::move(branch));
	guard->ElseMutable() = make_uniq<FunctionExpression>("error", std::move(error_args));
	return std::move(guard);
}

unique_ptr<ParsedExpression> UsingFor(GrantBook &grants, const string &schema, const string &table, CrossingVerb verb) {
	lock_guard<mutex> guard(grants.lock);
	auto grant = FindGrant(grants.tables, schema, table);
	return grant ? grant->CopyUsing(verb) : nullptr;
}

bool Grants(GrantBook &grants, const string &schema, const string &table, CrossingVerb verb) {
	lock_guard<mutex> guard(grants.lock);
	auto grant = FindGrant(grants.tables, schema, table);
	return grant && grant->Has(verb);
}

unique_ptr<ParsedExpression> CheckFor(GrantBook &grants, const string &schema, const string &table, CrossingVerb verb) {
	lock_guard<mutex> guard(grants.lock);
	auto grant = FindGrant(grants.tables, schema, table);
	return grant ? grant->CopyCheck(verb) : nullptr;
}

unique_ptr<ParsedExpression> And(unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right) {
	if (!left) {
		return right;
	}
	return make_uniq_base<ParsedExpression, ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left),
	                                                               std::move(right));
}

unique_ptr<ParsedExpression> Filled(unique_ptr<ParsedExpression> predicate,
                                    const case_insensitive_map_t<Value> &values) {
	if (predicate) {
		FillContext(predicate, values);
	}
	return predicate;
}

//! Stands the table behind its select policy: a floor with a policy binds to
//! `SELECT *, rowid FROM catalog.schema.table WHERE <using>` (a view has no rowid), one without binds to the table.
unique_ptr<TableRef> PolicyFloor(ClientContext &context, GrantBook &grants, const case_insensitive_map_t<Value> &values,
                                 const string &catalog, const CrossingFloor &floor) {
	if (floor.verb != CrossingVerb::SELECT && !floor.reads_rows) {
		return nullptr;
	}
	unique_ptr<ParsedExpression> using_predicate;
	{
		lock_guard<mutex> guard(grants.lock);
		auto grant = FindGrant(grants.tables, floor.schema, floor.table);
		if (!grant || !grant->Has(CrossingVerb::SELECT)) {
			throw PermissionException("vcatbridge: '%s.%s' does not have 'select' permission", floor.schema,
			                          floor.table);
		}
		if (grant->select_unrestricted) {
			return nullptr;
		}
		using_predicate = grant->CopyUsing(CrossingVerb::SELECT);
	}
	if (!using_predicate) {
		return nullptr;
	}
	FillContext(using_predicate, values);
	auto entry = SourceTableEntry(context, catalog, floor.schema, floor.table);
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->SetQualifiedName(Identifier(catalog), Identifier(floor.schema), Identifier(floor.table));
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	if (entry && entry->type == CatalogType::TABLE_ENTRY) {
		node->select_list.push_back(make_uniq<ColumnRefExpression>(Identifier("rowid")));
	}
	node->from_table = std::move(table_ref);
	node->where_clause = std::move(using_predicate);
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(node);
	return make_uniq<SubqueryRef>(std::move(select), Identifier(floor.table));
}

//! The columns an insert without a column list writes, in the order it writes them.
vector<string> InsertedColumns(ClientContext &context, const string &catalog, const string &schema,
                               const string &table) {
	auto entry = SourceTableEntry(context, catalog, schema, table);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException("vcatbridge: '%s.%s' is no longer on the source", schema, table);
	}
	vector<string> columns;
	for (auto &column : entry->Cast<TableCatalogEntry>().GetColumns().Logical()) {
		if (!column.Generated()) {
			columns.push_back(column.GetName().GetIdentifierName());
		}
	}
	return columns;
}

case_insensitive_map_t<const ParsedExpression *>
WrittenValues(const vector<string> &columns, const vector<unique_ptr<ParsedExpression>> &values, const string &table) {
	if (columns.size() != values.size()) {
		throw PermissionException("vcatbridge: a write to '%s' names %llu columns but writes %llu values", table,
		                          static_cast<uint64_t>(columns.size()), static_cast<uint64_t>(values.size()));
	}
	case_insensitive_map_t<const ParsedExpression *> written;
	for (idx_t c = 0; c < columns.size(); c++) {
		written[columns[c]] = values[c].get();
	}
	return written;
}

vector<string> NamesOf(const vector<Identifier> &identifiers) {
	vector<string> names;
	for (auto &identifier : identifiers) {
		names.push_back(identifier.GetIdentifierName());
	}
	return names;
}

//! The check guards the first written value with error(), so a violating row fails the statement.
void GuardWrittenValues(unique_ptr<ParsedExpression> check, const vector<string> &columns,
                        vector<unique_ptr<ParsedExpression>> &values, optional_ptr<const Identifier> alias,
                        const string &table) {
	if (!check) {
		return;
	}
	if (values.empty()) {
		throw PermissionException("vcatbridge: a write of default values to '%s' cannot be held to its check "
		                          "predicate",
		                          table);
	}
	PointAtWrittenRow(check, WrittenValues(columns, values, table), alias, table);
	values[0] = GuardWithCheck(std::move(check), std::move(values[0]));
}

struct WriteTarget {
	string schema;
	string table;
	Identifier alias;
};

WriteTarget WriteTargetOf(const TableRef &ref) {
	auto &name = ref.Cast<BaseTableRef>().GetQualifiedName();
	return {name.Schema().GetIdentifierName(), name.Name().GetIdentifierName(), ref.alias};
}

//! Grafts the write policies onto a write far is about to bind: the using predicate narrows which rows an update
//! or delete touches, the check guards the values written. The select policy narrows an update or delete already,
//! through PolicyFloor.
void ShapeWrite(ClientContext &context, const string &catalog, GrantBook &grants,
                const case_insensitive_map_t<Value> &values, SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::INSERT_STATEMENT: {
		auto &insert = *statement.Cast<InsertStatement>().node;
		auto schema = insert.qualified_name.Schema().GetIdentifierName();
		auto table = insert.qualified_name.Name().GetIdentifierName();
		auto check = Filled(CheckFor(grants, schema, table, CrossingVerb::INSERT), values);
		if (!check) {
			return;
		}
		if (!insert.select_statement) {
			throw PermissionException("vcatbridge: a write of default values to '%s' cannot be held to its check "
			                          "predicate",
			                          table);
		}
		auto columns =
		    insert.columns.empty() ? InsertedColumns(context, catalog, schema, table) : NamesOf(insert.columns);
		auto &select = insert.select_statement->node->Cast<SelectNode>();
		GuardWrittenValues(std::move(check), columns, select.select_list, nullptr, table);
		return;
	}
	case StatementType::UPDATE_STATEMENT: {
		auto &update = *statement.Cast<UpdateStatement>().node;
		auto target = WriteTargetOf(*update.table);
		auto &set_info = *update.set_info;
		auto using_predicate = Filled(UsingFor(grants, target.schema, target.table, CrossingVerb::UPDATE), values);
		if (using_predicate) {
			QualifyColumns(using_predicate, target.alias, target.table);
			set_info.condition = And(std::move(set_info.condition), std::move(using_predicate));
		}
		GuardWrittenValues(Filled(CheckFor(grants, target.schema, target.table, CrossingVerb::UPDATE), values),
		                   NamesOf(set_info.columns), set_info.expressions, &target.alias, target.table);
		return;
	}
	case StatementType::DELETE_STATEMENT: {
		auto &del = *statement.Cast<DeleteStatement>().node;
		auto target = WriteTargetOf(*del.table);
		auto using_predicate = Filled(UsingFor(grants, target.schema, target.table, CrossingVerb::DELETE_), values);
		if (using_predicate) {
			QualifyColumns(using_predicate, target.alias, target.table);
			del.condition = And(std::move(del.condition), std::move(using_predicate));
		}
		return;
	}
	case StatementType::MERGE_INTO_STATEMENT: {
		auto &merge = *statement.Cast<MergeIntoStatement>().node;
		auto target = WriteTargetOf(*merge.target);
		for (auto &when : merge.actions) {
			for (auto &action : when.second) {
				switch (action->action_type) {
				case MergeActionType::MERGE_UPDATE: {
					auto using_predicate =
					    Filled(UsingFor(grants, target.schema, target.table, CrossingVerb::UPDATE), values);
					if (using_predicate) {
						QualifyColumns(using_predicate, target.alias, target.table);
						action->condition = And(std::move(action->condition), std::move(using_predicate));
					}
					auto &set_info = *action->update_info;
					GuardWrittenValues(
					    Filled(CheckFor(grants, target.schema, target.table, CrossingVerb::UPDATE), values),
					    NamesOf(set_info.columns), set_info.expressions, &target.alias, target.table);
					break;
				}
				case MergeActionType::MERGE_DELETE: {
					auto using_predicate =
					    Filled(UsingFor(grants, target.schema, target.table, CrossingVerb::DELETE_), values);
					if (using_predicate) {
						QualifyColumns(using_predicate, target.alias, target.table);
						action->condition = And(std::move(action->condition), std::move(using_predicate));
					}
					break;
				}
				case MergeActionType::MERGE_INSERT: {
					auto check = Filled(CheckFor(grants, target.schema, target.table, CrossingVerb::INSERT), values);
					if (!check) {
						break;
					}
					auto columns = action->insert_columns.empty()
					                   ? InsertedColumns(context, catalog, target.schema, target.table)
					                   : NamesOf(action->insert_columns);
					GuardWrittenValues(std::move(check), columns, action->expressions, nullptr, target.table);
					break;
				}
				default:
					break;
				}
			}
		}
		return;
	}
	default:
		throw InternalException("vcatbridge: far was handed a %s as a write", StatementTypeToString(statement.type));
	}
}

string RequiredAttachOption(AttachInfo &info, const char *key) {
	auto it = info.options.find(key);
	if (it == info.options.end()) {
		throw IOException("vcatbridge: ATTACH (TYPE %s) requires a '%s' option", VCATBRIDGE_TYPE, key);
	}
	auto text = StringValue::Get(it->second.DefaultCastAs(LogicalType::VARCHAR));
	if (text.empty()) {
		throw IOException("vcatbridge: the '%s' ATTACH option cannot be empty", key);
	}
	return text;
}

const string &BridgePrefix() {
	static const string prefix = "vcatbridge: ";
	return prefix;
}

string StripBridgePrefix(string message) {
	if (StringUtil::StartsWith(message, BridgePrefix())) {
		message = message.substr(BridgePrefix().size());
	}
	return message;
}

[[noreturn]] void RethrowAt(const std::exception &ex, const string &path) {
	ErrorData error(ex);
	auto message = StripBridgePrefix(error.RawMessage());
	if (StringUtil::StartsWith(message, path)) {
		error.Throw();
	}
	ErrorData(error.Type(), BridgePrefix() + path + ": " + message).Throw();
}

struct PolicyIssue {
	string path;
	string severity;
	string message;
};

using PolicyIssues = optional_ptr<vector<PolicyIssue>>;

//! Null `issues` makes every failure throw; otherwise each failure is recorded and checking goes on, except
//! INTERRUPT, INTERNAL and FATAL, which always rethrow.
template <class F>
bool Guarded(PolicyIssues issues, const string &path, const char *severity, F &&f) {
	try {
		f();
		return true;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!issues) {
			RethrowAt(ex, path);
		}
		if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
		    error.Type() == ExceptionType::FATAL) {
			throw;
		}
		auto message = StripBridgePrefix(error.RawMessage());
		if (!path.empty() && StringUtil::StartsWith(message, path + ": ")) {
			message = message.substr(path.size() + 2);
		}
		issues->push_back({path, severity, std::move(message)});
		return false;
	}
}

template <class F>
void WithSourceTransaction(DatabaseInstance &db, const SourcePin &source, F &&f) {
	Connection source_conn(db);
	source_conn.BeginTransaction();
	try {
		auto catalog = source.Use(*source_conn.context);
		f(source_conn, catalog);
	} catch (...) {
		source_conn.Rollback();
		throw;
	}
	source_conn.Rollback();
}

//! An unset catalog matches the bridge's source catalog; a set one must equal it.
void ThrowIfOtherCatalog(const PolicyName &name, const string &catalog) {
	if (!name.catalog.empty() && !StringUtil::CIEquals(name.catalog, catalog)) {
		throw IOException("vcatbridge: '%s' names catalog '%s', but this bridge's source catalog is '%s'",
		                  name.ToString(), name.catalog, catalog);
	}
}

CatalogEntry &GrantableEntry(Connection &source_conn, const string &catalog, const PolicyName &name) {
	ThrowIfOtherCatalog(name, catalog);
	auto entry = SourceTableEntry(*source_conn.context, catalog, name.schema, name.name);
	if (!IsGrantable(entry)) {
		throw CatalogException("vcatbridge: '%s.%s' not found in catalog '%s' on source", name.schema, name.name,
		                       catalog);
	}
	return *entry;
}

void ThrowUnlessSchemaExists(Connection &source_conn, const string &catalog, const PolicyName &name) {
	ThrowIfOtherCatalog(name, catalog);
	EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, QualifiedName(Identifier(name.schema)));
	if (!Catalog::GetCatalog(*source_conn.context, Identifier(catalog))
	         .GetSchema(*source_conn.context, lookup, OnEntryNotFound::RETURN_NULL)) {
		throw CatalogException("vcatbridge: schema '%s' not found in catalog '%s' on source", name.schema, catalog);
	}
}

//! What the source says about a granted table, recorded with its grant.
struct TableFacts {
	vector<string> discovered_key;
	vector<string> foreign_key_parents;
};

TableFacts FactsOf(CatalogEntry &entry) {
	TableFacts facts;
	if (entry.type == CatalogType::TABLE_ENTRY) {
		facts.discovered_key = DiscoverKeyColumns(entry.Cast<TableCatalogEntry>());
		facts.foreign_key_parents = DiscoverForeignKeyParents(entry.Cast<TableCatalogEntry>());
	}
	return facts;
}

struct ResolvedVerb {
	bool granted = false;
	unique_ptr<ParsedExpression> using_predicate;
	unique_ptr<ParsedExpression> check_predicate;
	bool unrestricted = false;
};

//! An empty `check` reuses the USING predicate on update and is an error on insert.
ResolvedVerb ResolveVerb(Connection &source_conn, CatalogEntry &entry, const string &catalog, const PolicyName &name,
                         CrossingVerb verb, const PolicyVerb &policy,
                         optional_ptr<const case_insensitive_map_t<Value>> values) {
	ResolvedVerb resolved;
	resolved.granted = true;
	if (entry.type == CatalogType::VIEW_ENTRY && verb != CrossingVerb::SELECT) {
		throw IOException("vcatbridge: '%s.%s' is a view; a view can only be granted 'select', not '%s'", name.schema,
		                  name.name, CrossingVerbName(verb));
	}
	if (((IsDdlVerb(verb) && verb != CrossingVerb::ALTER) || verb == CrossingVerb::INSERT) &&
	    !IsUnrestricted(policy.using_text)) {
		throw IOException("vcatbridge: the USING predicate for '%s' must be literally 'true'", CrossingVerbName(verb));
	}
	// A failed bind aborts the transaction it ran in, which must not take the next setting's check with it.
	Connection probe_conn(*source_conn.context->db);
	probe_conn.BeginTransaction();
	auto source_table = SourceRelation(probe_conn, entry, catalog, name.schema, name.name);
	resolved.using_predicate = ParseAndValidatePredicate(source_table, policy.using_text, "USING", values);
	if (!policy.check_text.empty()) {
		resolved.check_predicate = ParseAndValidatePredicate(source_table, policy.check_text, "WITH CHECK", values);
	} else if (verb == CrossingVerb::INSERT) {
		throw IOException("vcatbridge: an 'insert' grant must state a check predicate");
	} else if (verb == CrossingVerb::UPDATE) {
		resolved.check_predicate = resolved.using_predicate->Copy();
	}
	if (verb == CrossingVerb::ALTER && MentionsColumn(*resolved.using_predicate)) {
		throw IOException("vcatbridge: the 'alter' predicate '%s' may only use context ($name), not a "
		                  "column",
		                  policy.using_text);
	}
	if (IsDdlVerb(verb) && verb != CrossingVerb::ALTER) {
		resolved.using_predicate.reset();
	}
	resolved.unrestricted = verb == CrossingVerb::SELECT && IsUnrestricted(policy.using_text);
	return resolved;
}

vector<string> RunKeyQuery(ClientContext &context, const string &query, const string &qualified) {
	Connection source_conn(*context.db);
	auto submitted = source_conn.Submit(query);
	if (submitted->HasError()) {
		submitted->GetErrorObject().Throw("vcatbridge: columns_from_query for '" + qualified + "' failed: ");
	}
	QueryResultStream<ChunkFormat> key_result(std::move(submitted));
	vector<string> key;
	while (true) {
		auto chunk = key_result.Fetch();
		if (key_result.HasError()) {
			key_result.GetErrorObject().Throw("vcatbridge: columns_from_query for '" + qualified + "' failed: ");
		}
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t row = 0; row < chunk->size(); row++) {
			auto value = chunk->GetValue(0, row);
			if (value.IsNull() || value.ToString().empty()) {
				throw IOException("vcatbridge: columns_from_query for '%s' returned an empty or NULL "
				                  "column name",
				                  qualified);
			}
			key.push_back(value.ToString());
		}
	}
	if (key.empty()) {
		throw IOException("vcatbridge: columns_from_query for '%s' returned no columns", qualified);
	}
	return key;
}

//! The source checks the claim, not the bridge: any row back means the key is not unique.
void RunVerifyUnique(ClientContext &context, const string &query, const string &qualified) {
	Connection source_conn(*context.db);
	auto submitted = source_conn.Submit(query);
	if (submitted->HasError()) {
		submitted->GetErrorObject().Throw("vcatbridge: verify_unique for '" + qualified + "' failed: ");
	}
	QueryResultStream<ChunkFormat> check_result(std::move(submitted));
	auto chunk = check_result.Fetch();
	if (check_result.HasError()) {
		check_result.GetErrorObject().Throw("vcatbridge: verify_unique for '" + qualified + "' failed: ");
	}
	if (chunk && chunk->size() > 0) {
		throw ConstraintException("vcatbridge: verify_unique for '%s' returned rows; the key is not "
		                          "unique on the source",
		                          qualified);
	}
}

struct ResolvedKey {
	bool set = false;
	vector<string> columns;
	bool verified = true;
};

ResolvedKey ResolveKey(ClientContext &context, CatalogEntry &entry, const PolicyName &name, const PolicyKey &key) {
	ResolvedKey resolved;
	resolved.set = true;
	auto qualified = name.schema + "." + name.name;
	if (entry.type != CatalogType::TABLE_ENTRY) {
		throw IOException("vcatbridge: '%s' is a view; a view has no primary key", qualified);
	}
	if (key.columns_from_query.empty()) {
		resolved.columns = key.columns;
	} else {
		resolved.columns = RunKeyQuery(context, key.columns_from_query, qualified);
	}
	auto &columns = entry.Cast<TableCatalogEntry>().GetColumns();
	for (auto &column : resolved.columns) {
		if (!columns.ColumnExists(Identifier(column))) {
			throw IOException("vcatbridge: '%s' is not a column of '%s'", column, qualified);
		}
	}
	resolved.verified = false;
	if (!key.verify_unique.empty()) {
		RunVerifyUnique(context, key.verify_unique, qualified);
		resolved.verified = true;
	}
	return resolved;
}

void ThrowUnlessForeignKey(Connection &source_conn, CatalogEntry &entry, const string &catalog, const PolicyName &child,
                           const PolicyName &parent) {
	ThrowIfOtherCatalog(parent, catalog);
	if (entry.type != CatalogType::TABLE_ENTRY) {
		throw IOException("vcatbridge: '%s.%s' is a view; a view has no foreign keys", child.schema, child.name);
	}
	auto parents = DiscoverForeignKeyParents(entry.Cast<TableCatalogEntry>());
	auto references = [&](const string &table) {
		return StringUtil::CIEquals(table, parent.name);
	};
	if (std::none_of(parents.begin(), parents.end(), references)) {
		throw IOException("vcatbridge: '%s.%s' has no foreign key to '%s.%s'", child.schema, child.name, parent.schema,
		                  parent.name);
	}
}

void InstallFacts(SourceGrant &grant, TableFacts facts) {
	grant.discovered_key = std::move(facts.discovered_key);
	grant.foreign_key_parents = std::move(facts.foreign_key_parents);
	if (!grant.key_set) {
		grant.key = grant.discovered_key;
	}
}

void InstallVerb(SourceGrant &grant, CrossingVerb verb, ResolvedVerb resolved) {
	auto bit = static_cast<uint8_t>(1u << static_cast<uint8_t>(verb));
	grant.verbs = resolved.granted ? static_cast<uint8_t>(grant.verbs | bit) : static_cast<uint8_t>(grant.verbs & ~bit);
	grant.using_predicates[static_cast<idx_t>(verb)] = std::move(resolved.using_predicate);
	grant.check_predicates[static_cast<idx_t>(verb)] = std::move(resolved.check_predicate);
	if (verb == CrossingVerb::SELECT) {
		grant.select_unrestricted = resolved.unrestricted;
	}
}

void InstallKey(SourceGrant &grant, ResolvedKey resolved) {
	grant.key_set = resolved.set;
	grant.key = resolved.set ? std::move(resolved.columns) : grant.discovered_key;
	grant.key_verified = resolved.set ? resolved.verified : true;
}

struct PolicyState {
	GrantTables tables;
	case_insensitive_set_t create_schemas;
	FunctionGrants functions;
};

PolicyState CopyPolicy(const GrantTables &tables, const case_insensitive_set_t &create_schemas,
                       const FunctionGrants &functions) {
	PolicyState state;
	for (auto &schema_entry : tables) {
		for (auto &table_entry : schema_entry.second) {
			state.tables[schema_entry.first][table_entry.first] = table_entry.second.Copy();
		}
	}
	state.create_schemas = create_schemas;
	state.functions = functions;
	return state;
}

//! The checks that need the whole grant set. Before ATTACH they wait for it, since a policy is built a
//! setting at a time; after ATTACH every change must leave a consistent set.
void ValidateGrantSet(GrantTables &tables, const case_insensitive_set_t &create_schemas, const string &bridge_id,
                      bool attaching) {
	idx_t table_count = create_schemas.size();
	for (auto &schema_entry : tables) {
		for (auto &table_entry : schema_entry.second) {
			auto &grant = table_entry.second;
			if (grant.verbs == 0) {
				throw PermissionException("vcatbridge: '%s.%s' in bridge '%s' sets primary_key or "
				                          "foreign_keys but grants no verb",
				                          schema_entry.first, table_entry.first, bridge_id);
			}
			table_count++;
			for (auto verb : {CrossingVerb::INSERT, CrossingVerb::UPDATE}) {
				if (grant.Has(verb)) {
					ThrowIfForeignKeyOracle(tables, schema_entry.first, table_entry.first, grant.foreign_key_parents,
					                        grant.allowed_foreign_keys, verb, bridge_id);
				}
			}
		}
	}
	if (attaching && table_count == 0) {
		throw PermissionException("vcatbridge: bridge '%s' has no policies; call vcatbridge_configure_policy "
		                          "or vcatbridge_set_policy on the source connection before attaching",
		                          bridge_id);
	}
}

//! Runs `edit(state, live)` on a copy of the bridge's policy and installs the result. Source lookups happen
//! before this, outside the registry lock, so `edit` only rearranges what is already resolved.
template <class F>
void CommitPolicy(const string &bridge_id, ClientContext &context, bool touches_tables, F &&edit) {
	PendingLock lock;
	auto target = LookupOpenGrants(lock, bridge_id, context);
	if (target.pending) {
		auto &pending = *target.pending;
		auto state = CopyPolicy(pending.schemas, pending.create_schemas, pending.functions);
		edit(state, false);
		pending.schemas = std::move(state.tables);
		pending.create_schemas = std::move(state.create_schemas);
		pending.functions = std::move(state.functions);
		return;
	}
	auto &book = *target.live;
	lock_guard<mutex> guard(book.lock);
	if (touches_tables && book.uncommitted_ddl > 0) {
		throw PermissionException("vcatbridge: a target transaction has uncommitted DDL on bridge '%s'; "
		                          "change the policy after it ends",
		                          bridge_id);
	}
	auto state = CopyPolicy(book.tables, book.create_schemas, book.functions);
	edit(state, true);
	ValidateGrantSet(state.tables, state.create_schemas, bridge_id, false);
	book.tables = std::move(state.tables);
	book.create_schemas = std::move(state.create_schemas);
	book.functions = std::move(state.functions);
	if (book.target) {
		book.target->Refresh();
	}
}

//! After ATTACH a table without verbs would be served as one, so settings other than verbs need a granted table.
SourceGrant &GrantToConfigure(PolicyState &state, const PolicyName &name, bool live, const string &bridge_id) {
	if (!live) {
		return state.tables[name.schema][name.name];
	}
	auto grant = FindGrant(state.tables, name.schema, name.name);
	if (!grant) {
		throw CatalogException("vcatbridge: '%s.%s' is not granted in bridge '%s'; grant it a verb first", name.schema,
		                       name.name, bridge_id);
	}
	return *grant;
}

void ApplyFunctionList(FunctionGrants &functions, PolicyListOp op, const vector<string> &names) {
	if (op == PolicyListOp::SET) {
		functions.builtins = false;
		functions.allowed.clear();
		functions.disallowed.clear();
	}
	for (auto &name : names) {
		auto allow = op != PolicyListOp::REMOVE;
		(allow ? functions.allowed : functions.disallowed).insert(name);
		(allow ? functions.disallowed : functions.allowed).erase(name);
	}
}

struct LookedUpBridge {
	shared_ptr<DatabaseInstance> source_db;
	SourcePin source;
	case_insensitive_map_t<Value> values;
};

LookedUpBridge LookUpOpenBridge(const string &bridge_id, ClientContext &context) {
	LookedUpBridge out;
	shared_ptr<BridgeContext> bridge_context;
	{
		PendingLock lock;
		auto target = LookupOpenGrants(lock, bridge_id, context);
		out.source_db = target.source_db;
		out.source = target.source;
		bridge_context = target.context;
	}
	out.values = ContextSnapshot(*bridge_context);
	return out;
}

void SetPolicy(ClientContext &context, const string &bridge_id, const PolicyEdit &edit) {
	auto bridge = LookUpOpenBridge(bridge_id, context);
	switch (edit.kind) {
	case PolicyEditKind::WHITELIST:
		CommitPolicy(bridge_id, context, false,
		             [&](PolicyState &state, bool) { ApplyFunctionList(state.functions, edit.op, edit.functions); });
		return;
	case PolicyEditKind::CREATE:
		if (edit.verb_value.granted) {
			WithSourceTransaction(*bridge.source_db, bridge.source,
			                      [&](Connection &source_conn, const string &catalog) {
				                      ThrowUnlessSchemaExists(source_conn, catalog, edit.target);
			                      });
		}
		CommitPolicy(bridge_id, context, true, [&](PolicyState &state, bool) {
			if (edit.verb_value.granted) {
				state.create_schemas.insert(edit.target.schema);
			} else {
				state.create_schemas.erase(edit.target.schema);
			}
		});
		return;
	case PolicyEditKind::VERB: {
		ResolvedVerb resolved;
		TableFacts facts;
		if (edit.verb_value.granted) {
			WithSourceTransaction(*bridge.source_db, bridge.source,
			                      [&](Connection &source_conn, const string &catalog) {
				                      auto &entry = GrantableEntry(source_conn, catalog, edit.target);
				                      resolved = ResolveVerb(source_conn, entry, catalog, edit.target, edit.verb,
				                                             edit.verb_value, &bridge.values);
				                      facts = FactsOf(entry);
			                      });
		}
		CommitPolicy(bridge_id, context, true, [&](PolicyState &state, bool) {
			if (!resolved.granted) {
				auto grant = FindGrant(state.tables, edit.target.schema, edit.target.name);
				if (!grant) {
					return;
				}
				InstallVerb(*grant, edit.verb, std::move(resolved));
				// A table without verbs is not granted, whatever else it set.
				if (grant->verbs == 0) {
					auto &schema_tables = state.tables[edit.target.schema];
					schema_tables.erase(edit.target.name);
					if (schema_tables.empty()) {
						state.tables.erase(edit.target.schema);
					}
				}
				return;
			}
			auto &grant = state.tables[edit.target.schema][edit.target.name];
			InstallFacts(grant, std::move(facts));
			InstallVerb(grant, edit.verb, std::move(resolved));
		});
		return;
	}
	case PolicyEditKind::PRIMARY_KEY: {
		ResolvedKey resolved;
		if (edit.has_key) {
			WithSourceTransaction(*bridge.source_db, bridge.source,
			                      [&](Connection &source_conn, const string &catalog) {
				                      auto &entry = GrantableEntry(source_conn, catalog, edit.target);
				                      resolved = ResolveKey(context, entry, edit.target, edit.key);
			                      });
		}
		CommitPolicy(bridge_id, context, true, [&](PolicyState &state, bool live) {
			if (!edit.has_key && !FindGrant(state.tables, edit.target.schema, edit.target.name)) {
				return;
			}
			InstallKey(GrantToConfigure(state, edit.target, live, bridge_id), std::move(resolved));
		});
		return;
	}
	case PolicyEditKind::FOREIGN_KEYS: {
		if (edit.op != PolicyListOp::REMOVE) {
			WithSourceTransaction(*bridge.source_db, bridge.source,
			                      [&](Connection &source_conn, const string &catalog) {
				                      auto &entry = GrantableEntry(source_conn, catalog, edit.target);
				                      for (auto &parent : edit.foreign_keys) {
					                      ThrowUnlessForeignKey(source_conn, entry, catalog, edit.target, parent);
				                      }
			                      });
		}
		CommitPolicy(bridge_id, context, true, [&](PolicyState &state, bool live) {
			bool clears =
			    edit.op == PolicyListOp::REMOVE || (edit.op == PolicyListOp::SET && edit.foreign_keys.empty());
			if (clears && !FindGrant(state.tables, edit.target.schema, edit.target.name)) {
				return;
			}
			auto &grant = GrantToConfigure(state, edit.target, live, bridge_id);
			if (edit.op == PolicyListOp::SET) {
				grant.allowed_foreign_keys.clear();
			}
			for (auto &parent : edit.foreign_keys) {
				if (edit.op == PolicyListOp::REMOVE) {
					grant.allowed_foreign_keys.erase(parent.name);
				} else {
					grant.allowed_foreign_keys.insert(parent.name);
				}
			}
		});
		return;
	}
	}
}

bool UsesContext(const ResolvedVerb &resolved) {
	case_insensitive_set_t names;
	for (auto predicate : {resolved.using_predicate.get(), resolved.check_predicate.get()}) {
		if (predicate) {
			ContextNames(*predicate, names);
		}
	}
	return !names.empty();
}

PolicyState ResolvePolicy(ClientContext &context, Connection &source_conn, const string &catalog,
                          const PolicyConfig &config, optional_ptr<const case_insensitive_map_t<Value>> values,
                          PolicyIssues issues) {
	PolicyState state;
	for (auto &schema : config.create_schemas) {
		if (Guarded(issues, schema.path, "error",
		            [&]() { ThrowUnlessSchemaExists(source_conn, catalog, schema.name); })) {
			state.create_schemas.insert(schema.name.schema);
		}
	}
	for (auto &table : config.tables) {
		optional_ptr<CatalogEntry> entry;
		auto found = Guarded(issues, table.path, "error", [&]() {
			if (FindGrant(state.tables, table.name.schema, table.name.name)) {
				throw IOException("vcatbridge: '%s.%s' is configured twice", table.name.schema, table.name.name);
			}
			entry = &GrantableEntry(source_conn, catalog, table.name);
		});
		if (!found) {
			continue;
		}
		SourceGrant grant;
		InstallFacts(grant, FactsOf(*entry));
		for (auto verb : CrossingVerbs()) {
			auto &policy = table.verbs[static_cast<idx_t>(verb)];
			if (!policy.granted) {
				continue;
			}
			auto path = table.path + "." + CrossingVerbName(verb);
			Guarded(issues, path, "error", [&]() {
				auto resolved = ResolveVerb(source_conn, *entry, catalog, table.name, verb, policy, values);
				if (issues && !values && UsesContext(resolved)) {
					issues->push_back({path, "unchecked", "the predicate uses context, so it was not bound"});
				}
				InstallVerb(grant, verb, std::move(resolved));
			});
		}
		if (table.has_key) {
			auto severity =
			    table.key.columns_from_query.empty() && table.key.verify_unique.empty() ? "error" : "data_dependent";
			Guarded(issues, table.path + ".primary_key", severity,
			        [&]() { InstallKey(grant, ResolveKey(context, *entry, table.name, table.key)); });
		}
		for (auto &parent : table.foreign_keys) {
			if (Guarded(issues, table.path + ".foreign_keys", "error",
			            [&]() { ThrowUnlessForeignKey(source_conn, *entry, catalog, table.name, parent); })) {
				grant.allowed_foreign_keys.insert(parent.name);
			}
		}
		if (issues && grant.verbs == 0) {
			continue;
		}
		state.tables[table.name.schema][table.name.name] = std::move(grant);
	}
	state.functions.builtins = config.functions.builtins;
	ApplyFunctionList(state.functions, PolicyListOp::ADD, config.functions.add);
	ApplyFunctionList(state.functions, PolicyListOp::REMOVE, config.functions.remove);
	return state;
}

void ConfigurePolicy(ClientContext &context, const string &bridge_id, const PolicyConfig &config) {
	auto bridge = LookUpOpenBridge(bridge_id, context);
	PolicyState state;
	WithSourceTransaction(*bridge.source_db, bridge.source, [&](Connection &source_conn, const string &catalog) {
		state = ResolvePolicy(context, source_conn, catalog, config, &bridge.values, nullptr);
	});
	CommitPolicy(bridge_id, context, true, [&](PolicyState &current, bool) { current = std::move(state); });
}

void SetPolicyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto has_op = args.ColumnCount() == 5;
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto value_column = has_op ? 4 : 3;
		for (idx_t c = 0; c < static_cast<idx_t>(value_column); c++) {
			if (args.data[c].GetValue(i).IsNull()) {
				throw IOException("vcatbridge: argument %llu of vcatbridge_set_policy cannot be NULL",
				                  static_cast<uint64_t>(c + 1));
			}
		}
		auto bridge_id = StringValue::Get(args.data[0].GetValue(i));
		auto target = StringValue::Get(args.data[1].GetValue(i));
		auto setting = StringValue::Get(args.data[2].GetValue(i));
		string op;
		if (has_op) {
			op = StringValue::Get(args.data[3].GetValue(i));
		}
		auto edit = ParsePolicyEdit(target, setting, has_op ? &op : nullptr, args.data[value_column].GetValue(i));
		try {
			SetPolicy(context, bridge_id, edit);
		} catch (std::exception &ex) {
			RethrowAt(ex, edit.path);
		}
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void ConfigurePolicyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten();
	args.data[1].Flatten();
	ThrowIfAnyNull(args, 2, "vcatbridge_configure_policy");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto documents = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		ConfigurePolicy(context, bridge_ids[i].GetString(), ParsePolicyJson(documents[i].GetString()));
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

struct ValidatePolicyBindData : public TableFunctionData {
	vector<PolicyIssue> issues;
};

struct ValidatePolicyState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> ValidatePolicyBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"path", "severity", "message"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	for (idx_t c = 0; c < input.inputs.size(); c++) {
		if (input.inputs[c].IsNull()) {
			throw IOException("vcatbridge: argument %llu of vcatbridge_validate_policy_config cannot be NULL",
			                  static_cast<uint64_t>(c + 1));
		}
	}
	auto source_catalog = StringValue::Get(input.inputs[0]);
	auto document = StringValue::Get(input.inputs[1]);
	auto source = PinSourceCatalog(context, source_catalog);
	auto result = make_uniq<ValidatePolicyBindData>();
	auto issues = PolicyIssues(&result->issues);
	PolicyConfig config;
	if (!Guarded(issues, "", "error", [&]() { config = ParsePolicyJson(document); })) {
		return std::move(result);
	}
	PolicyState state;
	WithSourceTransaction(*context.db, source, [&](Connection &source_conn, const string &catalog) {
		state = ResolvePolicy(context, source_conn, catalog, config, nullptr, issues);
	});
	auto empty_document = config.tables.empty() && config.create_schemas.empty();
	Guarded(issues, "", "error",
	        [&]() { ValidateGrantSet(state.tables, state.create_schemas, "<unregistered>", empty_document); });
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ValidatePolicyInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<ValidatePolicyState>();
}

void ValidatePolicyScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ValidatePolicyBindData>();
	auto &state = data_p.global_state->Cast<ValidatePolicyState>();
	idx_t count = 0;
	while (state.offset < bind_data.issues.size() && count < STANDARD_VECTOR_SIZE) {
		auto &issue = bind_data.issues[state.offset++];
		output.data[0].SetValue(count, issue.path.empty() ? Value(LogicalType::VARCHAR) : Value(issue.path));
		output.data[1].SetValue(count, Value(issue.severity));
		output.data[2].SetValue(count, Value(issue.message));
		count++;
	}
	output.SetChildCardinality(count);
}

struct ContextDependent {
	string schema;
	string table;
	const char *clause;
	unique_ptr<ParsedExpression> predicate;
};

struct ContextTarget {
	shared_ptr<BridgeContext> context;
	SourcePin source;
	vector<ContextDependent> dependents;
};

void CollectDependents(const case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> &tables, const string &name,
                       vector<ContextDependent> &out) {
	auto uses_name = [&](const unique_ptr<ParsedExpression> &predicate) {
		if (!predicate) {
			return false;
		}
		case_insensitive_set_t names;
		ContextNames(*predicate, names);
		return names.count(name) > 0;
	};
	for (auto &schema_entry : tables) {
		for (auto &table_entry : schema_entry.second) {
			auto &grant = table_entry.second;
			for (idx_t v = 0; v < grant.using_predicates.size(); v++) {
				if (uses_name(grant.using_predicates[v])) {
					out.push_back({schema_entry.first, table_entry.first, "USING", grant.using_predicates[v]->Copy()});
				}
				if (uses_name(grant.check_predicates[v])) {
					out.push_back(
					    {schema_entry.first, table_entry.first, "WITH CHECK", grant.check_predicates[v]->Copy()});
				}
			}
		}
	}
}

//! Caller holds PendingMutex. A bridge's context outlives its pending entry: once attached it is found
//! through LiveBridges until the DuckDBSource is destroyed.
ContextTarget LookupContext(PendingLock &lock, const string &bridge_id, ClientContext &context, const string &name) {
	ContextTarget target;
	PurgeExpiredPendingSources(lock);
	if (PendingSources().count(bridge_id)) {
		auto &pending = LookupPendingForGrant(lock, bridge_id, context);
		target.context = pending.context;
		target.source = pending.source;
		if (!name.empty()) {
			CollectDependents(pending.schemas, name, target.dependents);
		}
		return target;
	}
	auto grants = LookupLive(bridge_id, context, target.source);
	target.context = grants->context;
	if (!name.empty()) {
		lock_guard<mutex> guard(grants->lock);
		CollectDependents(grants->tables, name, target.dependents);
	}
	return target;
}

void SetContextFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto id_value = args.data[0].GetValue(i);
		auto name_value = args.data[1].GetValue(i);
		auto value = args.data[2].GetValue(i);
		if (id_value.IsNull() || name_value.IsNull()) {
			throw IOException("vcatbridge: vcatbridge_set_context needs a bridge id and a context name");
		}
		auto bridge_id = StringValue::Get(id_value);
		auto name = StringValue::Get(name_value);
		if (name.empty()) {
			throw IOException("vcatbridge: the context name cannot be empty");
		}
		if (value.IsNull()) {
			throw IOException("vcatbridge: context '%s' cannot be NULL", name);
		}
		ContextTarget target;
		{
			PendingLock lock;
			target = LookupContext(lock, bridge_id, context, name);
		}
		auto values = ContextSnapshot(*target.context);
		if (target.context->finalized) {
			throw PermissionException("vcatbridge: the context of bridge '%s' is finalized", bridge_id);
		}
		values[name] = value;
		if (!target.dependents.empty()) {
			WithSourceTransaction(*context.db, target.source, [&](Connection &source_conn, const string &catalog) {
				for (auto &dependent : target.dependents) {
					auto entry = SourceTableEntry(*source_conn.context, catalog, dependent.schema, dependent.table);
					if (!IsGrantable(entry)) {
						continue;
					}
					auto source_table = SourceRelation(source_conn, *entry, catalog, dependent.schema, dependent.table);
					ProbePredicate(source_table, *dependent.predicate, values, dependent.predicate->ToString(),
					               dependent.clause);
				}
			});
		}
		lock_guard<mutex> guard(target.context->lock);
		if (target.context->finalized) {
			throw PermissionException("vcatbridge: the context of bridge '%s' is finalized", bridge_id);
		}
		target.context->values[name] = std::move(value);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void FinalizeContextFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten();
	ThrowIfAnyNull(args, 1, "vcatbridge_finalize_context");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		PendingLock lock;
		auto target = LookupContext(lock, bridge_id, context, string());
		lock_guard<mutex> guard(target.context->lock);
		if (target.context->finalized) {
			throw PermissionException("vcatbridge: the context of bridge '%s' is already finalized", bridge_id);
		}
		target.context->finalized = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void FinalizePolicyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten();
	ThrowIfAnyNull(args, 1, "vcatbridge_finalize_policy");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		PendingLock lock;
		auto target = LookupGrantTarget(lock, bridge_id, context);
		if (*target.policy_finalized) {
			throw PermissionException("vcatbridge: the policies of bridge '%s' are already finalized", bridge_id);
		}
		*target.policy_finalized = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

bool IsConstant(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
		return true;
	case ExpressionClass::CAST:
		return IsConstant(expr.Cast<CastExpression>().Child());
	default:
		return false;
	}
}

void ThrowUnlessConstantDefault(const ParsedExpression &expr) {
	if (!IsConstant(expr)) {
		throw PermissionException("vcatbridge: a default from the target must be a constant, not '%s'",
		                          expr.ToString());
	}
}

void ThrowIfTypeChangeHasUsing(const ChangeColumnTypeInfo &change) {
	if (!change.expression) {
		return;
	}
	auto &expr = *change.expression;
	if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		auto &child = expr.Cast<CastExpression>().Child();
		if (child.GetExpressionClass() == ExpressionClass::COLUMN_REF && change.column_path.size() == 1 &&
		    child.Cast<ColumnRefExpression>().GetColumnName() == change.column_path[0]) {
			return;
		}
	}
	throw PermissionException("vcatbridge: a type change from the target cannot take a USING "
	                          "expression, not '%s'",
	                          expr.ToString());
}

void ThrowIfColumnRunsOnSource(const ColumnDefinition &column) {
	if (column.Generated()) {
		throw PermissionException("vcatbridge: a table from the target cannot declare a generated "
		                          "column ('%s')",
		                          column.GetName());
	}
	if (column.HasDefaultValue()) {
		ThrowUnlessConstantDefault(column.DefaultValue());
	}
}

Value CurrentSetting(const DatabaseInstance &db, const string &name) {
	Value value;
	if (!db.TryGetCurrentSetting(Identifier(name), value)) {
		return Value();
	}
	return value;
}

void ThrowUnlessSettingsMatch(const DatabaseInstance &source_db, const DatabaseInstance &target_db,
                              const string &bridge_id) {
	for (auto &name : MatchedSettings()) {
		auto source_value = CurrentSetting(source_db, name);
		auto target_value = CurrentSetting(target_db, name);
		if (!Value::NotDistinctFrom(source_value, target_value)) {
			throw InvalidInputException("vcatbridge: bridge '%s' needs setting '%s' to match on source "
			                            "and target, but the source has %s and the target has %s",
			                            bridge_id, name, source_value.ToSQLString(), target_value.ToSQLString());
		}
	}
}

} // namespace

unique_ptr<DuckDBSource> RedeemBridge(const string &bridge_id, const string &token, DatabaseInstance &target_db) {
	auto colon = token.find(':');
	if (colon != string::npos && token.substr(0, colon) != LoadNonce()) {
		throw PermissionException(
		    "vcatbridge: source and target loaded different copies of the virtual_catalog extension. "
		    "Ensure both connections use the same extension installation.");
	}

	shared_ptr<DatabaseInstance> source_db;
	SourcePin source;
	auto grants = make_shared_ptr<GrantBook>();
	{
		PendingLock lock;
		PurgeExpiredPendingSources(lock);
		auto it = PendingSources().find(bridge_id);
		if (it == PendingSources().end()) {
			throw PermissionException("vcatbridge: no registered source for bridge '%s'", bridge_id);
		}
		if (!ConstantTimeEquals(it->second.token, token)) {
			if (++it->second.token_failures >= MAX_TOKEN_FAILURES) {
				lock.Drop(it);
			}
			throw PermissionException("vcatbridge: invalid setup token for bridge '%s'", bridge_id);
		}
		if (IsLiveBridge(bridge_id)) {
			throw PermissionException("vcatbridge: bridge '%s' is already attached", bridge_id);
		}
		ValidateGrantSet(it->second.schemas, it->second.create_schemas, bridge_id, true);
		source_db = lock.Pin(it->second);
		if (!source_db) {
			lock.Drop(it);
			throw PermissionException("vcatbridge: no registered source for bridge '%s'", bridge_id);
		}
		ThrowUnlessSettingsMatch(*source_db, target_db, bridge_id);
		source = it->second.source;
		grants->tables = std::move(it->second.schemas);
		grants->create_schemas = std::move(it->second.create_schemas);
		grants->functions = std::move(it->second.functions);
		grants->context = std::move(it->second.context);
		grants->policy_finalized = it->second.policy_finalized;
		PendingSources().erase(it);
		LiveBridges()[bridge_id] = LiveBridge {source_db.get(), source, grants};
	}
	return make_uniq<DuckDBSource>(bridge_id, source_db, target_db, std::move(source), std::move(grants));
}

unique_ptr<DuckDBSource> RedeemBridgeAttach(AttachInfo &info, DatabaseInstance &target_db) {
	return RedeemBridge(RequiredAttachOption(info, "id"), RequiredAttachOption(info, "token"), target_db);
}

SourcePin::SourcePin(AttachedDatabase &attached_p, string registered_name_p)
    : attached(attached_p.shared_from_this()), registered_name(std::move(registered_name_p)) {
}

string SourcePin::Use(ClientContext &source_context) const {
	auto pinned = attached.lock();
	if (!pinned || DatabaseManager::Get(source_context).GetDatabase(pinned->GetName()) != pinned) {
		throw CatalogException("vcatbridge: source catalog '%s' was detached", registered_name);
	}
	auto name = pinned->GetName();
	auto &transaction = MetaTransaction::Get(source_context);
	auto referenced = transaction.GetReferencedDatabase(name);
	if (referenced && referenced.get() != pinned.get()) {
		throw CatalogException("vcatbridge: source catalog '%s' was detached", registered_name);
	}
	transaction.UseDatabase(pinned);
	return name.GetIdentifierName();
}

DuckDBSource::DuckDBSource(string bridge_id_p, const shared_ptr<DatabaseInstance> &source_db_p,
                           DatabaseInstance &target_db, SourcePin source_p, shared_ptr<GrantBook> grants_p)
    : bridge_id(std::move(bridge_id_p)), source_db(source_db_p),
      source_db_hold(source_db_p.get() == &target_db ? nullptr : source_db_p), source(std::move(source_p)),
      grants(std::move(grants_p)), bound(true) {
}

DuckDBSource::DuckDBSource() : bound(false) {
}

void DuckDBSource::Bind(DuckDBSource &redeemed) {
	if (bound) {
		throw InternalException("vcatbridge: bridge is already bound");
	}
	bridge_id = std::move(redeemed.bridge_id);
	source_db = redeemed.source_db;
	redeemed.source_db.reset();
	source_db_hold = std::move(redeemed.source_db_hold);
	source = std::move(redeemed.source);
	grants = std::move(redeemed.grants);
	redeemed.bound = false;
	bound = true;
}

bool DuckDBSource::IsBound() const {
	return bound;
}

void DuckDBSource::ThrowUnlessBound() const {
	if (!bound) {
		throw PermissionException("vcatbridge: not initialized; call vcatbridge_init(bridge_id, token)");
	}
}

shared_ptr<DatabaseInstance> DuckDBSource::Database() const {
	auto db = source_db.lock();
	if (!db) {
		throw ConnectionException("vcatbridge: the source database of bridge '%s' is closed", bridge_id);
	}
	return db;
}

DuckDBSource::~DuckDBSource() {
	if (!grants) {
		return;
	}
	lock_guard<mutex> lock(PendingMutex());
	auto it = LiveBridges().find(bridge_id);
	if (it != LiveBridges().end() && it->second.grants.lock() == grants) {
		LiveBridges().erase(it);
	}
}

void DuckDBSource::SetTarget(optional_ptr<CrossingAttach> target) {
	if (!grants) {
		return;
	}
	lock_guard<mutex> guard(grants->lock);
	grants->target = target;
}

vector<string> DuckDBSource::Schemas() {
	ThrowUnlessBound();
	lock_guard<mutex> guard(grants->lock);
	case_insensitive_set_t names;
	for (auto &schema : grants->tables) {
		names.insert(schema.first);
	}
	for (auto &schema : grants->create_schemas) {
		names.insert(schema);
	}
	vector<string> out(names.begin(), names.end());
	std::sort(out.begin(), out.end());
	return out;
}

vector<string> DuckDBSource::Tables(const string &schema) {
	ThrowUnlessBound();
	lock_guard<mutex> guard(grants->lock);
	vector<string> out;
	auto it = grants->tables.find(schema);
	if (it == grants->tables.end()) {
		return out;
	}
	for (auto &table : it->second) {
		out.push_back(table.first);
	}
	std::sort(out.begin(), out.end());
	return out;
}

CrossingSchema DuckDBSource::DescribeSchema(const string &schema) {
	ThrowUnlessBound();
	lock_guard<mutex> guard(grants->lock);
	CrossingSchema described;
	described.name = schema;
	if (grants->create_schemas.count(schema)) {
		described.verbs.push_back(CrossingVerb::CREATE);
	}
	return described;
}

vector<CrossingVerb> DuckDBSource::GrantedVerbs(const string &schema, const string &name) {
	lock_guard<mutex> guard(grants->lock);
	vector<CrossingVerb> verbs;
	auto grant = FindGrant(grants->tables, schema, name);
	if (!grant) {
		return verbs;
	}
	for (auto verb : CrossingVerbs()) {
		if (grant->Has(verb)) {
			verbs.push_back(verb);
		}
	}
	return verbs;
}

vector<string> DuckDBSource::GrantedKey(const string &schema, const string &name) {
	ThrowUnlessBound();
	lock_guard<mutex> guard(grants->lock);
	auto grant = FindGrant(grants->tables, schema, name);
	return grant ? grant->key : vector<string>();
}

CrossingTable DuckDBSource::Describe(const string &schema, const string &name) {
	ThrowUnlessBound();
	{
		lock_guard<mutex> guard(grants->lock);
		if (!FindGrant(grants->tables, schema, name)) {
			throw CatalogException("vcatbridge: '%s.%s' is not granted", schema, name);
		}
	}

	CrossingTable table;
	table.name = name;
	table.returning = true;
	const auto describe = [&](ClientContext &context, const string &catalog) {
		auto entry = SourceTableEntry(context, catalog, schema, name);
		if (!IsGrantable(entry)) {
			throw CatalogException("vcatbridge: '%s.%s' is no longer on the source", schema, name);
		}
		if (entry->type == CatalogType::VIEW_ENTRY) {
			auto &view = entry->Cast<ViewCatalogEntry>();
			view.BindView(context);
			auto columns = view.GetColumnInfo();
			auto names = BindContext::AliasColumnNames(Identifier(name), columns->names, view.aliases);
			for (idx_t i = 0; i < names.size(); i++) {
				table.Column(names[i].GetIdentifierName(), columns->types[i]);
			}
			table.row_ids = false;
			return;
		}
		auto &source_table = entry->Cast<TableCatalogEntry>();
		for (auto &column : source_table.GetColumns().Logical()) {
			table.Column(column.GetName().GetIdentifierName(), column.GetType());
		}
		for (auto &constraint : source_table.GetConstraints()) {
			if (constraint->type == ConstraintType::FOREIGN_KEY) {
				continue;
			}
			table.constraints.push_back(constraint->Copy());
		}
	};

	WithSourceTransaction(*Database(), source,
	                      [&](Connection &conn, const string &catalog) { describe(*conn.context, catalog); });
	table.verbs = GrantedVerbs(schema, name);
	return table;
}

void DuckDBSource::DescribeChanged(const string &schema, CrossingTable &table) {
	ThrowUnlessBound();
	CrossingSource::DescribeChanged(schema, table);
	table.verbs = GrantedVerbs(schema, table.name);
}

GrantLedger::GrantLedger(shared_ptr<GrantBook> grants_p) : grants(std::move(grants_p)) {
}

GrantLedger::~GrantLedger() {
	Resolve(false);
}

void GrantLedger::Push(std::function<void()> entry) {
	undo.push_back(std::move(entry));
	grants->uncommitted_ddl++;
}

void GrantLedger::Forget() {
	grants->uncommitted_ddl -= undo.size();
	undo.clear();
}

void GrantLedger::Resolve(bool committed) {
	lock_guard<mutex> guard(grants->lock);
	if (!committed) {
		for (auto it = undo.rbegin(); it != undo.rend(); ++it) {
			(*it)();
		}
	}
	Forget();
}

namespace {

struct DdlTarget {
	CrossingVerb verb;
	string schema;
	string table;
	optional_ptr<const CreateInfo> create;
	optional_ptr<const AlterInfo> alter;
	optional_ptr<const DropInfo> drop;
};

DdlTarget DdlTargetOf(const SQLStatement &statement) {
	DdlTarget ddl;
	QualifiedName name;
	switch (statement.type) {
	case StatementType::CREATE_STATEMENT:
		ddl.verb = CrossingVerb::CREATE;
		ddl.create = statement.Cast<CreateStatement>().info.get();
		name = ddl.create->GetQualifiedName();
		break;
	case StatementType::ALTER_STATEMENT:
		ddl.verb = CrossingVerb::ALTER;
		ddl.alter = statement.Cast<AlterStatement>().info.get();
		name = ddl.alter->GetQualifiedName();
		break;
	case StatementType::DROP_STATEMENT:
		ddl.verb = CrossingVerb::DROP;
		ddl.drop = statement.Cast<DropStatement>().info.get();
		name = ddl.drop->GetQualifiedName();
		break;
	default:
		throw InternalException("vcatbridge: far was handed a %s as DDL", StatementTypeToString(statement.type));
	}
	ddl.schema = name.Schema().GetIdentifierName();
	ddl.table = name.Name().GetIdentifierName();
	return ddl;
}

bool CreateWouldCreate(GrantBook &grants, ClientContext &context, const string &catalog, const DdlTarget &ddl) {
	auto existing = SourceTableEntry(context, catalog, ddl.schema, ddl.table);
	if (!existing) {
		return true;
	}
	switch (ddl.create->on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return false;
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		lock_guard<mutex> guard(grants.lock);
		if (!FindGrant(grants.tables, ddl.schema, ddl.table)) {
			throw PermissionException("vcatbridge: '%s' does not have '%s' permission", ddl.table,
			                          CrossingVerbName(CrossingVerb::DROP));
		}
		return true;
	}
	default:
		return true;
	}
}

//! Throws unless both the alter grant and the select policy fold to true under the current context: an ALTER
//! runs over every row on the source, including rows the select policy would hide from this target.
void ThrowUnlessAlterGrantedOverFullTable(GrantBook &grants, ClientContext &source_context, const string &catalog,
                                          const DdlTarget &ddl) {
	bool alter_granted = false;
	bool select_granted = false;
	unique_ptr<ParsedExpression> alter_using;
	unique_ptr<ParsedExpression> select_using;
	{
		lock_guard<mutex> guard(grants.lock);
		auto grant = FindGrant(grants.tables, ddl.schema, ddl.table);
		if (grant) {
			alter_granted = grant->Has(CrossingVerb::ALTER);
			select_granted = grant->Has(CrossingVerb::SELECT);
			alter_using = grant->CopyUsing(CrossingVerb::ALTER);
			select_using = grant->CopyUsing(CrossingVerb::SELECT);
		}
	}
	auto values = ContextSnapshot(*grants.context);
	const auto holds = [&](bool granted, unique_ptr<ParsedExpression> &predicate) {
		if (!granted || !predicate) {
			return granted;
		}
		FillContext(predicate, values);
		return PredicateFoldsToTrue(source_context, catalog, ddl.schema, ddl.table, *predicate);
	};
	if (!holds(alter_granted, alter_using)) {
		throw PermissionException("vcatbridge: '%s' does not have '%s' permission", ddl.table,
		                          CrossingVerbName(CrossingVerb::ALTER));
	}
	if (!holds(select_granted, select_using)) {
		throw PermissionException("vcatbridge: the select policy on '%s.%s' hides rows, so the target "
		                          "cannot see the full table and cannot alter it",
		                          ddl.schema, ddl.table);
	}
}

//! The source checks a foreign key over every referenced row, hidden ones included.
void ThrowIfCreateDeclaresForeignKey(const DdlTarget &ddl) {
	if (ddl.create->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	for (auto &constraint : ddl.create->Cast<CreateTableInfo>().constraints) {
		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			throw PermissionException("vcatbridge: a table created from the target cannot declare a foreign key");
		}
	}
}

//! Non-constant defaults, CHECKs and generated columns are stored on the source and run on the owner's later
//! inserts; an ALTER COLUMN TYPE ... USING cast runs on the source at once. Each would run over rows the
//! target cannot see.
void ThrowIfDdlCarriesExpression(const DdlTarget &ddl) {
	if (ddl.create && ddl.create->type == CatalogType::TABLE_ENTRY) {
		auto &create = ddl.create->Cast<CreateTableInfo>();
		for (auto &column : create.columns.Logical()) {
			ThrowIfColumnRunsOnSource(column);
		}
		for (auto &constraint : create.constraints) {
			if (constraint->type == ConstraintType::CHECK) {
				throw PermissionException(
				    "vcatbridge: a table created from the target cannot declare a CHECK constraint");
			}
		}
		return;
	}
	if (!ddl.alter || ddl.alter->type != AlterType::ALTER_TABLE) {
		return;
	}
	auto &alter = ddl.alter->Cast<AlterTableInfo>();
	switch (alter.alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		ThrowIfColumnRunsOnSource(alter.Cast<AddColumnInfo>().new_column);
		break;
	case AlterTableType::ADD_FIELD:
		ThrowIfColumnRunsOnSource(alter.Cast<AddFieldInfo>().new_field);
		break;
	case AlterTableType::SET_DEFAULT:
		if (alter.Cast<SetDefaultInfo>().expression) {
			ThrowUnlessConstantDefault(*alter.Cast<SetDefaultInfo>().expression);
		}
		break;
	case AlterTableType::ALTER_COLUMN_TYPE:
		ThrowIfTypeChangeHasUsing(alter.Cast<ChangeColumnTypeInfo>());
		break;
	default:
		break;
	}
}

void ThrowIfAlterOrphansPolicy(GrantBook &grants, const DdlTarget &ddl) {
	if (ddl.alter->type != AlterType::ALTER_TABLE) {
		return;
	}
	auto &alter = ddl.alter->Cast<AlterTableInfo>();
	string column;
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_COLUMN:
		column = alter.Cast<RenameColumnInfo>().old_name.GetIdentifierName();
		break;
	case AlterTableType::REMOVE_COLUMN:
		column = alter.Cast<RemoveColumnInfo>().removed_column.GetIdentifierName();
		break;
	case AlterTableType::ALTER_COLUMN_TYPE:
		column = alter.Cast<ChangeColumnTypeInfo>().column_path[0].GetIdentifierName();
		break;
	default:
		return;
	}
	lock_guard<mutex> guard(grants.lock);
	auto found = FindGrant(grants.tables, ddl.schema, ddl.table);
	if (!found) {
		return;
	}
	auto &grant = *found;
	bool orphaned = false;
	for (auto &key_column : grant.key) {
		orphaned = orphaned || StringUtil::CIEquals(key_column, column);
	}
	const auto mentions = [&](const ParsedExpression &expr) {
		bool found = false;
		std::function<void(const ParsedExpression &)> visit = [&](const ParsedExpression &node) {
			if (node.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			    node.Cast<ColumnRefExpression>().GetColumnName() == column) {
				found = true;
			}
			ParsedExpressionIterator::EnumerateChildren(node, visit);
		};
		visit(expr);
		return found;
	};
	for (idx_t v = 0; v < CrossingVerbs().size() && !orphaned; v++) {
		orphaned = (grant.using_predicates[v] && mentions(*grant.using_predicates[v])) ||
		           (grant.check_predicates[v] && mentions(*grant.check_predicates[v]));
	}
	if (orphaned) {
		throw IOException("vcatbridge: '%s' is the key or a policy column of '%s.%s'", column, ddl.schema, ddl.table);
	}
}

vector<string> DeclaredKeyColumns(const CreateInfo &create) {
	vector<string> key;
	if (create.type != CatalogType::TABLE_ENTRY) {
		return key;
	}
	auto &table = create.Cast<CreateTableInfo>();
	for (auto &constraint : table.constraints) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		if (unique.HasIndex()) {
			key.push_back(table.columns.GetColumn(unique.GetIndex()).GetName().GetIdentifierName());
		} else {
			for (auto &column : unique.GetColumnNames()) {
				key.push_back(column.GetIdentifierName());
			}
		}
		break;
	}
	return key;
}

void RecordDdl(const shared_ptr<GrantBook> &book, GrantLedger &ledger, const DdlTarget &ddl) {
	lock_guard<mutex> guard(book->lock);
	auto schema_name = ddl.schema;
	auto table_name = ddl.table;
	auto &schema = book->tables[schema_name];
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		SourceGrant grant;
		grant.discovered_key = DeclaredKeyColumns(*ddl.create);
		grant.key = grant.discovered_key;
		auto view = ddl.create->type == CatalogType::VIEW_ENTRY;
		for (auto verb : CrossingVerbs()) {
			if (view && verb != CrossingVerb::SELECT && verb != CrossingVerb::CREATE && verb != CrossingVerb::DROP) {
				continue;
			}
			grant.Allow(verb);
		}
		shared_ptr<SourceGrant> replaced;
		auto it = schema.find(table_name);
		if (it != schema.end()) {
			replaced = make_shared_ptr<SourceGrant>(std::move(it->second));
		}
		schema[table_name] = std::move(grant);
		ledger.Push([book, schema_name, table_name, replaced]() {
			if (replaced) {
				book->tables[schema_name][table_name] = std::move(*replaced);
			} else {
				book->tables[schema_name].erase(table_name);
			}
		});
		return;
	}
	case CrossingVerb::DROP: {
		auto it = schema.find(table_name);
		if (it == schema.end()) {
			return;
		}
		auto dropped = make_shared_ptr<SourceGrant>(std::move(it->second));
		schema.erase(it);
		ledger.Push([book, schema_name, table_name, dropped]() {
			book->tables[schema_name][table_name] = std::move(*dropped);
		});
		return;
	}
	default:
		break;
	}
	if (ddl.alter->type != AlterType::ALTER_TABLE) {
		return;
	}
	auto &alter = ddl.alter->Cast<AlterTableInfo>();
	if (alter.alter_table_type != AlterTableType::RENAME_TABLE) {
		return;
	}
	auto it = schema.find(table_name);
	if (it == schema.end()) {
		return;
	}
	auto new_name = alter.Cast<RenameTableInfo>().new_table_name.GetIdentifierName();
	auto moved = std::move(it->second);
	schema.erase(it);
	schema[new_name] = std::move(moved);
	ledger.Push([book, schema_name, table_name, new_name]() {
		auto &tables = book->tables[schema_name];
		auto renamed = tables.find(new_name);
		if (renamed == tables.end()) {
			return;
		}
		auto back = std::move(renamed->second);
		tables.erase(renamed);
		tables[table_name] = std::move(back);
	});
}

void ShapeDdl(ClientContext &context, const string &catalog, const shared_ptr<GrantBook> &grants, GrantLedger &ledger,
              SQLStatement &statement) {
	auto ddl = DdlTargetOf(statement);
	switch (ddl.verb) {
	case CrossingVerb::CREATE:
		ThrowIfCreateDeclaresForeignKey(ddl);
		ThrowIfDdlCarriesExpression(ddl);
		break;
	case CrossingVerb::ALTER:
		ThrowIfDdlCarriesExpression(ddl);
		ThrowUnlessAlterGrantedOverFullTable(*grants, context, catalog, ddl);
		ThrowIfAlterOrphansPolicy(*grants, ddl);
		break;
	default:
		break;
	}
	if (ddl.verb != CrossingVerb::CREATE || CreateWouldCreate(*grants, context, catalog, ddl)) {
		RecordDdl(grants, ledger, ddl);
	}
}

//! One far transaction on the source, served on a thread of its own under the bridge's policies.
class BridgeLink : public CrossingLink {
public:
	BridgeLink(shared_ptr<DatabaseInstance> db_p, SourcePin source_p, string catalog_p, shared_ptr<GrantBook> grants_p)
	    : db(std::move(db_p)), source(std::move(source_p)), catalog(std::move(catalog_p)), grants(std::move(grants_p)),
	      values(ContextSnapshot(*grants->context)), ledger(make_shared_ptr<GrantLedger>(grants)), connection(*db),
	      far(connection, catalog, Options()) {
		serving = std::thread([this]() {
			try {
				far.Serve(pair.Far());
			} catch (...) { // NOLINT: Serve has closed the link with the error; nothing on this thread can report it
			}
		});
	}
	~BridgeLink() override {
		if (serving.joinable()) {
			serving.join();
		}
	}

	idx_t Streams() override {
		return pair.Near().Streams();
	}
	unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override {
		return pair.Near().Open(id, receiver);
	}

	const shared_ptr<GrantLedger> &Ledger() const {
		return ledger;
	}

private:
	//! The far connection, with the pinned source bound into its transaction.
	ClientContext &Pinned() {
		auto &context = *connection.context;
		source.Use(context);
		return context;
	}

	CrossingFarOptions Options() {
		CrossingFarOptions options;
		options.catalogs = [this](const string &named) {
			auto pinned = source.Use(*connection.context);
			if (!StringUtil::CIEquals(named, pinned)) {
				throw PermissionException("vcatbridge: a plan names catalog '%s', but this bridge's source catalog "
				                          "is '%s'",
				                          named, pinned);
			}
		};
		options.floors = [this](const CrossingFloor &floor) {
			return PolicyFloor(Pinned(), *grants, values, catalog, floor);
		};
		options.writes = [this](SQLStatement &statement) {
			ShapeWrite(Pinned(), catalog, *grants, values, statement);
		};
		options.ddl = [this](SQLStatement &statement) {
			ShapeDdl(Pinned(), catalog, grants, *ledger, statement);
		};
		return options;
	}

	shared_ptr<DatabaseInstance> db;
	SourcePin source;
	string catalog;
	shared_ptr<GrantBook> grants;
	case_insensitive_map_t<Value> values;
	shared_ptr<GrantLedger> ledger;
	CrossingLinkPair pair;
	Connection connection;
	CrossingFar far;
	std::thread serving;
};

void CollectLedgers(const CrossingOpenTransaction &transaction, vector<shared_ptr<GrantLedger>> &out) {
	if (auto link = dynamic_cast<BridgeLink *>(transaction.link.get())) {
		out.push_back(link->Ledger());
	}
	for (auto &finished : transaction.finished) {
		CollectLedgers(*finished, out);
	}
}

} // namespace

vector<shared_ptr<GrantLedger>> GrantLedgersOf(const CrossingOpenTransaction &transaction) {
	vector<shared_ptr<GrantLedger>> out;
	CollectLedgers(transaction, out);
	return out;
}

unique_ptr<CrossingLink> DuckDBSource::Connect(ClientContext &) {
	ThrowUnlessBound();
	auto db = Database();
	string catalog;
	WithSourceTransaction(*db, source, [&](Connection &, const string &name) { catalog = name; });
	return make_uniq<BridgeLink>(std::move(db), source, std::move(catalog), grants);
}

shared_ptr<Connection> DuckDBSource::PlanningConnection() {
	auto planning = make_shared_ptr<Connection>(*Database());
	planning->BeginTransaction();
	return planning;
}

bool DuckDBSource::SourceHasFunction(const BoundSimpleFunction &function, CatalogType type) {
	auto planning_conn = PlanningConnection();
	auto &catalog = function.GetCatalogName();
	auto &schema = function.GetSchemaName();
	EntryLookupInfo lookup(type,
	                       QualifiedName(catalog.empty() ? Identifier(SYSTEM_CATALOG) : catalog,
	                                     schema.empty() ? Identifier(DEFAULT_SCHEMA) : schema, function.GetName()));
	return Catalog::GetEntry(*planning_conn->context, lookup, OnEntryNotFound::RETURN_NULL) != nullptr;
}

CrossingVerdict DuckDBSource::AcceptsCall(const Expression &expr) {
	ThrowUnlessBound();
	optional_ptr<const BoundSimpleFunction> function;
	CatalogType type;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		function = &expr.Cast<BoundFunctionExpression>().Function();
		type = CatalogType::SCALAR_FUNCTION_ENTRY;
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		function = &expr.Cast<BoundAggregateExpression>().Function();
		type = CatalogType::AGGREGATE_FUNCTION_ENTRY;
		break;
	case ExpressionClass::BOUND_WINDOW: {
		auto &aggregate = expr.Cast<BoundWindowExpression>().AggregateFunction();
		if (!aggregate) {
			return CrossingVerdict::Yes();
		}
		function = aggregate.get();
		type = CatalogType::AGGREGATE_FUNCTION_ENTRY;
		break;
	}
	default:
		return CrossingVerdict::Yes();
	}
	auto &name = function->GetName().GetIdentifierName();
	bool allowed;
	bool builtins;
	{
		lock_guard<mutex> guard(grants->lock);
		if (grants->functions.disallowed.count(name)) {
			return CrossingVerdict::No("bridge '" + bridge_id + "' keeps " + name + " on the target");
		}
		allowed = grants->functions.allowed.count(name) > 0;
		builtins = grants->functions.builtins;
	}
	if (!allowed && !(builtins && PushableBuiltins().count(name))) {
		return CrossingVerdict::No("bridge '" + bridge_id + "' keeps " + name + " on the target");
	}
	if (!SourceHasFunction(*function, type)) {
		return CrossingVerdict::No("the source has no function " + name);
	}
	return CrossingVerdict::Yes();
}

CrossingVerdict DuckDBSource::AcceptsType(const LogicalType &type) {
	ThrowUnlessBound();
	return CrossingVerdict::Yes();
}

void RegisterBridgeFunctions(ExtensionLoader &loader) {
	ScalarFunctionSet register_source_set("vcatbridge_register_source");
	ScalarFunction register_source(vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, RegisterSourceFunc);
	register_source.SetStability(FunctionStability::VOLATILE);
	register_source.SetFallible();
	register_source.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	register_source_set.AddFunction(register_source);
	ScalarFunction register_source_token(
	    vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    RegisterSourceFunc);
	register_source_token.SetStability(FunctionStability::VOLATILE);
	register_source_token.SetFallible();
	register_source_token.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	register_source_set.AddFunction(register_source_token);
	loader.RegisterFunction(register_source_set);

	ScalarFunctionSet set_policy_set("vcatbridge_set_policy");
	ScalarFunction set_policy(
	    vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
	    LogicalType::VARCHAR, SetPolicyFunc);
	set_policy.SetStability(FunctionStability::VOLATILE);
	set_policy.SetFallible();
	set_policy.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	set_policy_set.AddFunction(set_policy);
	ScalarFunction set_policy_list(vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
	                               LogicalType::VARCHAR, SetPolicyFunc);
	set_policy_list.SetStability(FunctionStability::VOLATILE);
	set_policy_list.SetFallible();
	set_policy_list.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	set_policy_set.AddFunction(set_policy_list);
	loader.RegisterFunction(set_policy_set);

	ScalarFunction configure_policy("vcatbridge_configure_policy",
	                                vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                LogicalType::VARCHAR, ConfigurePolicyFunc);
	configure_policy.SetStability(FunctionStability::VOLATILE);
	configure_policy.SetFallible();
	configure_policy.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(configure_policy);

	TableFunction validate_policy("vcatbridge_validate_policy_config", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              ValidatePolicyScan, ValidatePolicyBind, ValidatePolicyInitGlobal);
	loader.RegisterFunction(validate_policy);

	ScalarFunction set_context("vcatbridge_set_context",
	                           vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
	                           LogicalType::VARCHAR, SetContextFunc);
	set_context.SetStability(FunctionStability::VOLATILE);
	set_context.SetFallible();
	set_context.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(set_context);

	ScalarFunction finalize_context("vcatbridge_finalize_context", vector<LogicalType> {LogicalType::VARCHAR},
	                                LogicalType::VARCHAR, FinalizeContextFunc);
	finalize_context.SetStability(FunctionStability::VOLATILE);
	finalize_context.SetFallible();
	finalize_context.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(finalize_context);

	ScalarFunction finalize_policy("vcatbridge_finalize_policy", vector<LogicalType> {LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, FinalizePolicyFunc);
	finalize_policy.SetStability(FunctionStability::VOLATILE);
	finalize_policy.SetFallible();
	finalize_policy.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(finalize_policy);
}

} // namespace duckdb
