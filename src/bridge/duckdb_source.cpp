#include "duckdb_source.hpp"

#include "crossing_attach.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

struct PendingSource {
	shared_ptr<DatabaseInstance> source_db;
	string token;
	SourcePin source;
	std::chrono::steady_clock::time_point last_touched;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> schemas;
	case_insensitive_set_t create_schemas;
	shared_ptr<BridgeContext> context = make_shared_ptr<BridgeContext>();
	bool policy_finalized = false;
};

struct LiveBridge {
	DatabaseInstance *source_db;
	SourcePin source;
	weak_ptr<GrantBook> grants;
};

constexpr int64_t PENDING_SOURCE_TTL_SECONDS = 30;

//! Marks a connection a bridge opened for one autocommit read. A bridge whose source is another bridge
//! begins its session on that connection, which is inside a transaction only because the read needs
//! one; the marker lets it read in autocommit too, streaming instead of materializing its result.
class StreamingReadMarker : public ClientContextState {};

const string &StreamingReadMarkerKey() {
	static const int anchor = 0;
	static const string key =
	    "virtual_catalog_bridge/streaming_read/" + std::to_string(reinterpret_cast<uintptr_t>(&anchor));
	return key;
}

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

void PurgeExpiredPendingSources() {
	auto now = std::chrono::steady_clock::now();
	auto &pending = PendingSources();
	for (auto it = pending.begin(); it != pending.end();) {
		auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_touched).count();
		it = idle > PENDING_SOURCE_TTL_SECONDS ? pending.erase(it) : std::next(it);
	}
}

void SplitSchemaName(const string &name, string &schema) {
	if (name.find('.') != string::npos) {
		throw IOException("virtual_catalog_bridge: a 'create' policy names a schema, not a table; got '%s'", name);
	}
	if (name.empty()) {
		throw IOException("virtual_catalog_bridge: the schema name cannot be empty");
	}
	schema = name;
}

void SplitGrantName(const string &qualified, string &schema, string &table) {
	auto dot = qualified.find('.');
	if (dot == string::npos) {
		throw IOException("virtual_catalog_bridge: table name '%s' must be schema-qualified as 'schema.table'",
		                  qualified);
	}
	if (qualified.rfind('.') != dot) {
		throw IOException("virtual_catalog_bridge: table name '%s' must name a schema and a table "
		                  "('schema.table'); the source catalog is fixed by bridge_register_source",
		                  qualified);
	}
	schema = qualified.substr(0, dot);
	table = qualified.substr(dot + 1);
	if (schema.empty() || table.empty()) {
		throw IOException("virtual_catalog_bridge: table name '%s' has an empty schema or table name", qualified);
	}
}

PendingSource &LookupPendingForGrant(const string &bridge_id, ClientContext &context) {
	PurgeExpiredPendingSources();
	auto it = PendingSources().find(bridge_id);
	if (it == PendingSources().end()) {
		throw PermissionException("virtual_catalog_bridge: no registered source for bridge '%s'. "
		                          "Call bridge_register_source on the source connection first.",
		                          bridge_id);
	}
	if (it->second.source_db.get() != context.db.get()) {
		throw PermissionException("virtual_catalog_bridge: bridge '%s' was registered by a different source connection",
		                          bridge_id);
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
		throw PermissionException("virtual_catalog_bridge: no registered source for bridge '%s'", bridge_id);
	}
	if (it->second.source_db != context.db.get()) {
		throw PermissionException("virtual_catalog_bridge: bridge '%s' was registered by a different source connection",
		                          bridge_id);
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

//! Caller holds PendingMutex.
GrantTarget LookupGrantTarget(const string &bridge_id, ClientContext &context) {
	GrantTarget target;
	PurgeExpiredPendingSources();
	if (PendingSources().count(bridge_id)) {
		auto &pending = LookupPendingForGrant(bridge_id, context);
		target.source_db = pending.source_db;
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

//! Caller holds PendingMutex.
GrantTarget LookupOpenGrants(const string &bridge_id, ClientContext &context) {
	auto target = LookupGrantTarget(bridge_id, context);
	if (*target.policy_finalized) {
		throw PermissionException("virtual_catalog_bridge: the policies of bridge '%s' are finalized", bridge_id);
	}
	return target;
}

//! Runs `f(tables, create_schemas, live)` under the lock that guards the bridge's grants. With `refresh`, an
//! attached target is refreshed so its next statement sees the change.
template <class F>
void WithOpenGrants(const string &bridge_id, ClientContext &context, bool refresh, F &&f) {
	lock_guard<mutex> lock(PendingMutex());
	auto target = LookupOpenGrants(bridge_id, context);
	if (target.pending) {
		f(target.pending->schemas, target.pending->create_schemas, false);
		return;
	}
	lock_guard<mutex> guard(target.live->lock);
	f(target.live->tables, target.live->create_schemas, true);
	if (refresh && target.live->target) {
		target.live->target->Refresh();
	}
}

template <class F>
void ReadGrants(const string &bridge_id, ClientContext &context, F &&read) {
	WithOpenGrants(bridge_id, context, false, std::forward<F>(read));
}

template <class F>
void WriteGrants(const string &bridge_id, ClientContext &context, F &&write) {
	WithOpenGrants(bridge_id, context, true, std::forward<F>(write));
}

optional_ptr<SourceGrant> FindGrant(GrantTables &tables, const string &schema, const string &table) {
	auto schema_it = tables.find(schema);
	if (schema_it == tables.end()) {
		return nullptr;
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? nullptr : &table_it->second;
}

//! A key set after ATTACH must name a granted table: an entry without verbs would be served as one.
SourceGrant &GrantForKey(GrantTables &tables, const string &schema, const string &table, bool live,
                         const string &bridge_id) {
	if (!live) {
		return tables[schema][table];
	}
	auto grant = FindGrant(tables, schema, table);
	if (!grant) {
		throw CatalogException("virtual_catalog_bridge: '%s.%s' is not granted in bridge '%s'", schema, table,
		                       bridge_id);
	}
	return *grant;
}

void ThrowIfAnyNull(DataChunk &args, idx_t columns, const char *function) {
	for (idx_t c = 0; c < columns; c++) {
		auto &validity = FlatVector::Validity(args.data[c]);
		for (idx_t i = 0; i < args.size(); i++) {
			if (!validity.RowIsValid(i)) {
				throw IOException("virtual_catalog_bridge: argument %llu of %s cannot be NULL",
				                  static_cast<uint64_t>(c + 1), function);
			}
		}
	}
}

void RegisterSourceFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto arg_count = args.ColumnCount();
	for (idx_t c = 0; c < arg_count; c++) {
		args.data[c].Flatten(count);
	}
	ThrowIfAnyNull(args, arg_count, "bridge_register_source");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto source_catalogs = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto source_catalog = source_catalogs[i].GetString();
		if (bridge_id.empty()) {
			throw IOException("virtual_catalog_bridge: bridge id cannot be empty");
		}
		auto entry = Catalog::GetCatalogEntry(context, source_catalog);
		if (!entry) {
			throw CatalogException("virtual_catalog_bridge: source catalog '%s' does not exist", source_catalog);
		}
		if (entry->IsTemporaryCatalog() || entry->IsSystemCatalog()) {
			throw CatalogException("virtual_catalog_bridge: source catalog '%s' is a temporary or system catalog",
			                       source_catalog);
		}

		PendingSource pending;
		pending.source_db = context.db;
		if (arg_count > 2) {
			auto chosen = FlatVector::GetData<string_t>(args.data[2])[i].GetString();
			if (chosen.empty()) {
				throw IOException("virtual_catalog_bridge: the token cannot be empty");
			}
			pending.token = std::move(chosen);
		} else {
			pending.token = LoadNonce() + ":" + UUID::ToString(UUID::GenerateRandomUUID());
		}
		pending.source = SourcePin(entry->GetAttached(), source_catalog);
		pending.last_touched = std::chrono::steady_clock::now();

		lock_guard<mutex> lock(PendingMutex());
		PurgeExpiredPendingSources();
		if (PendingSources().find(bridge_id) != PendingSources().end()) {
			throw IOException("virtual_catalog_bridge: a source is already registered for bridge '%s'", bridge_id);
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
		return conn.Table(catalog, schema, name);
	}
	auto ref = make_uniq<BaseTableRef>();
	ref->catalog_name = catalog;
	ref->schema_name = schema;
	ref->table_name = name;
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

void ContextNames(const ParsedExpression &expr, case_insensitive_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::PARAMETER) {
		names.insert(expr.Cast<ParameterExpression>().identifier);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](const ParsedExpression &child) { ContextNames(child, names); });
}

void FillContext(unique_ptr<ParsedExpression> &expr, const case_insensitive_map_t<Value> &values) {
	if (expr->GetExpressionClass() == ExpressionClass::PARAMETER) {
		auto &name = expr->Cast<ParameterExpression>().identifier;
		auto it = values.find(name);
		if (it == values.end()) {
			throw InternalException("virtual_catalog_bridge: context '$%s' is not set", name);
		}
		expr = make_uniq<ConstantExpression>(it->second);
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
		throw IOException("virtual_catalog_bridge: the %s predicate '%s' is not a valid WHERE clause on the "
		                  "source: %s",
		                  clause, text, ex.what());
	}
	auto &columns = probe->Columns();
	if (columns.size() != 1) {
		throw IOException("virtual_catalog_bridge: the %s predicate '%s' must be a single expression", clause, text);
	}
	if (columns[0].Type().id() != LogicalTypeId::BOOLEAN) {
		throw IOException("virtual_catalog_bridge: the %s predicate '%s' must be BOOLEAN, but it is %s", clause, text,
		                  columns[0].Type().ToString());
	}
}

unique_ptr<ParsedExpression> ParseAndValidatePredicate(const shared_ptr<Relation> &source_table, const string &text,
                                                       const char *clause,
                                                       const case_insensitive_map_t<Value> &values) {
	auto parsed = Parser::ParseExpressionList(text);
	if (parsed.size() != 1) {
		throw IOException("virtual_catalog_bridge: the %s predicate must be a single expression", clause);
	}
	auto expr = std::move(parsed[0]);
	case_insensitive_set_t names;
	ContextNames(*expr, names);
	for (auto &name : names) {
		if (std::all_of(name.begin(), name.end(), StringUtil::CharacterIsDigit)) {
			throw IOException("virtual_catalog_bridge: the %s predicate '%s' may only use named context "
			                  "parameters ($name), not positional ones",
			                  clause, text);
		}
		if (!values.count(name)) {
			throw IOException("virtual_catalog_bridge: the %s predicate '%s' uses context '$%s', which is not set; "
			                  "call bridge_set_context before bridge_policy",
			                  clause, text, name);
		}
	}
	ClearQueryLocations(*expr);
	ProbePredicate(source_table, *expr, values, text, clause);
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
			key.push_back(table.GetColumns().GetColumn(unique.GetIndex()).GetName());
		} else {
			key = unique.GetColumnNames();
		}
		break;
	}
	return key;
}

//! Rewrites every unqualified column reference in `expr`, the root included: a set column becomes
//! its seam column (the value being written), anything else is pinned to the target table.
void PointCheckAtUpdatedRow(unique_ptr<ParsedExpression> &expr, const string &table, const vector<string> &set_columns,
                            const CrossingWriteStatement &built, idx_t key_count) {
	if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_ref = expr->Cast<ColumnRefExpression>();
		if (!column_ref.IsQualified()) {
			auto &name = column_ref.GetColumnName();
			for (idx_t c = 0; c < set_columns.size(); c++) {
				if (StringUtil::CIEquals(set_columns[c], name)) {
					expr = make_uniq<ColumnRefExpression>(built.seam_columns[key_count + c], built.seam_alias);
					return;
				}
			}
			expr = make_uniq<ColumnRefExpression>(name, table);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		PointCheckAtUpdatedRow(child, table, set_columns, built, key_count);
	});
}

void QualifyColumns(unique_ptr<ParsedExpression> &expr, const string &table, const CrossingWriteStatement &built) {
	PointCheckAtUpdatedRow(expr, table, {}, built, 0);
}

unique_ptr<ParsedExpression> GuardWithCheck(unique_ptr<ParsedExpression> check, unique_ptr<ParsedExpression> value) {
	vector<unique_ptr<ParsedExpression>> error_args;
	error_args.push_back(
	    make_uniq<ConstantExpression>(Value("virtual_catalog_bridge: row violates the check predicate on this table")));
	auto guard = make_uniq<CaseExpression>();
	CaseCheck branch;
	branch.when_expr = std::move(check);
	branch.then_expr = std::move(value);
	guard->case_checks.push_back(std::move(branch));
	guard->else_expr = make_uniq<FunctionExpression>("error", std::move(error_args));
	return std::move(guard);
}

unique_ptr<ParsedExpression> UsingFor(GrantBook &grants, const string &schema, const string &table, CrossingVerb verb) {
	lock_guard<mutex> guard(grants.lock);
	auto schema_it = grants.tables.find(schema);
	if (schema_it == grants.tables.end()) {
		return nullptr;
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? nullptr : table_it->second.CopyUsing(verb);
}

unique_ptr<ParsedExpression> CheckFor(GrantBook &grants, const string &schema, const string &table, CrossingVerb verb) {
	lock_guard<mutex> guard(grants.lock);
	auto schema_it = grants.tables.find(schema);
	if (schema_it == grants.tables.end()) {
		return nullptr;
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? nullptr : table_it->second.CopyCheck(verb);
}

unique_ptr<ParsedExpression> And(unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right) {
	if (!left) {
		return right;
	}
	return make_uniq_base<ParsedExpression, ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left),
	                                                               std::move(right));
}

//! Stands the table behind its select policy: a floor with a policy binds to
//! `SELECT <columns> FROM catalog.schema.table WHERE <using>`, one without binds to the table.
CrossingFloorResolver PolicyResolver(const shared_ptr<GrantBook> &grants, const string &source_catalog) {
	auto values = ContextSnapshot(*grants->context);
	return [grants, source_catalog, values](const CrossingFloor &floor) -> unique_ptr<TableRef> {
		auto using_predicate = UsingFor(*grants, floor.schema, floor.table, CrossingVerb::SELECT);
		if (!using_predicate) {
			return nullptr;
		}
		FillContext(using_predicate, values);
		auto table_ref = make_uniq<BaseTableRef>();
		table_ref->catalog_name = source_catalog;
		table_ref->schema_name = floor.schema;
		table_ref->table_name = floor.table;
		auto node = make_uniq<SelectNode>();
		for (auto &column : floor.column_names) {
			node->select_list.push_back(make_uniq<ColumnRefExpression>(column, floor.table));
		}
		node->from_table = std::move(table_ref);
		node->where_clause = std::move(using_predicate);
		auto select = make_uniq<SelectStatement>();
		select->node = std::move(node);
		return make_uniq<SubqueryRef>(std::move(select), floor.table);
	};
}

//! Grafts the write policies onto the statement crossing built: the using predicate narrows which
//! rows an update or delete touches, the check guards the first written value with error() so a
//! violating row fails the statement.
CrossingWriteShaper PolicyShaper(const shared_ptr<GrantBook> &grants, const CrossingWriteTarget &target) {
	auto values = ContextSnapshot(*grants->context);
	return [grants, target, values](CrossingWriteStatement &built) {
		auto using_predicate = UsingFor(*grants, target.schema, target.table, target.verb);
		auto check = CheckFor(*grants, target.schema, target.table, target.verb);
		if (using_predicate) {
			FillContext(using_predicate, values);
		}
		if (check) {
			FillContext(check, values);
		}
		switch (target.verb) {
		case CrossingVerb::INSERT: {
			if (!check) {
				return;
			}
			auto &select = built.statement->Cast<InsertStatement>().select_statement->node->Cast<SelectNode>();
			PointCheckAtUpdatedRow(check, target.table, target.set_columns, built, 0);
			select.select_list[0] = GuardWithCheck(std::move(check), std::move(select.select_list[0]));
			return;
		}
		case CrossingVerb::UPDATE: {
			auto &set_info = *built.statement->Cast<UpdateStatement>().set_info;
			if (using_predicate) {
				QualifyColumns(using_predicate, target.table, built);
				set_info.condition = And(std::move(set_info.condition), std::move(using_predicate));
			}
			if (check) {
				PointCheckAtUpdatedRow(check, target.table, target.set_columns, built, target.key_columns.size());
				set_info.expressions[0] = GuardWithCheck(std::move(check), std::move(set_info.expressions[0]));
			}
			return;
		}
		case CrossingVerb::DELETE_: {
			auto &statement = built.statement->Cast<DeleteStatement>();
			if (using_predicate) {
				QualifyColumns(using_predicate, target.table, built);
				statement.condition = And(std::move(statement.condition), std::move(using_predicate));
			}
			return;
		}
		default:
			return;
		}
	};
}

void PolicyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto arg_count = args.ColumnCount();
	for (idx_t c = 0; c < arg_count; c++) {
		args.data[c].Flatten(count);
	}
	ThrowIfAnyNull(args, 4, "bridge_policy");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto table_names = FlatVector::GetData<string_t>(args.data[1]);
	auto verbs = FlatVector::GetData<string_t>(args.data[2]);
	auto usings = FlatVector::GetData<string_t>(args.data[3]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		CrossingVerb verb;
		if (!TryParseCrossingVerb(verbs[i].GetString(), verb)) {
			throw IOException("virtual_catalog_bridge: invalid verb '%s'", verbs[i].GetString());
		}
		auto has_check = arg_count > 4 && FlatVector::Validity(args.data[4]).RowIsValid(i);
		if (has_check && verb != CrossingVerb::INSERT && verb != CrossingVerb::UPDATE) {
			throw IOException("virtual_catalog_bridge: a WITH CHECK predicate is only supported for 'insert' and "
			                  "'update', not '%s'; it is what a write may leave behind",
			                  CrossingVerbName(verb));
		}
		auto using_text = usings[i].GetString();
		if ((IsDdlVerb(verb) || verb == CrossingVerb::INSERT) && !IsUnrestricted(using_text)) {
			throw IOException("virtual_catalog_bridge: the USING predicate for '%s' must be literally 'true'",
			                  CrossingVerbName(verb));
		}

		string schema;
		string table;
		if (verb == CrossingVerb::CREATE) {
			SplitSchemaName(table_names[i].GetString(), schema);
		} else {
			SplitGrantName(table_names[i].GetString(), schema, table);
		}

		shared_ptr<DatabaseInstance> source_db;
		SourcePin source;
		case_insensitive_map_t<Value> values;
		{
			lock_guard<mutex> lock(PendingMutex());
			auto target = LookupOpenGrants(bridge_id, context);
			source_db = target.source_db;
			source = target.source;
			values = ContextSnapshot(*target.context);
		}

		auto is_view = false;
		vector<string> discovered_key;
		unique_ptr<ParsedExpression> using_predicate;
		unique_ptr<ParsedExpression> check_predicate;
		{
			Connection source_conn(*source_db);
			source_conn.BeginTransaction();
			try {
				auto catalog = source.Use(*source_conn.context);
				if (verb == CrossingVerb::CREATE) {
					EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, schema);
					auto entry =
					    Catalog::GetSchema(*source_conn.context, catalog, lookup, OnEntryNotFound::RETURN_NULL);
					if (!entry) {
						throw CatalogException(
						    "virtual_catalog_bridge: schema '%s' not found in catalog '%s' on source", schema, catalog);
					}
				} else {
					EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
					auto entry =
					    Catalog::GetEntry(*source_conn.context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
					if (!IsGrantable(entry)) {
						throw CatalogException("virtual_catalog_bridge: '%s.%s' not found in catalog '%s' on source",
						                       schema, table, catalog);
					}
					is_view = entry->type == CatalogType::VIEW_ENTRY;
					if (is_view && verb != CrossingVerb::SELECT) {
						throw IOException(
						    "virtual_catalog_bridge: '%s.%s' is a view; a view can only be granted 'select', not '%s'",
						    schema, table, CrossingVerbName(verb));
					}

					auto source_table = SourceRelation(source_conn, *entry, catalog, schema, table);
					using_predicate = ParseAndValidatePredicate(source_table, using_text, "USING", values);
					if (has_check) {
						auto checks = FlatVector::GetData<string_t>(args.data[4]);
						check_predicate =
						    ParseAndValidatePredicate(source_table, checks[i].GetString(), "WITH CHECK", values);
					} else if (verb == CrossingVerb::INSERT) {
						throw IOException(
						    "virtual_catalog_bridge: an 'insert' grant must state a WITH CHECK predicate");
					} else if (verb == CrossingVerb::UPDATE) {
						check_predicate = using_predicate->Copy();
					}
					if (IsDdlVerb(verb)) {
						using_predicate.reset();
					}
					if (!is_view) {
						discovered_key = DiscoverKeyColumns(entry->Cast<TableCatalogEntry>());
					}
				}
			} catch (...) {
				source_conn.Rollback();
				throw;
			}
			source_conn.Rollback();
		}

		WriteGrants(bridge_id, context, [&](GrantTables &tables, case_insensitive_set_t &create_schemas, bool live) {
			if (verb == CrossingVerb::CREATE) {
				if (create_schemas.count(schema)) {
					throw IOException("virtual_catalog_bridge: a 'create' policy is already defined for schema '%s'",
					                  schema);
				}
				create_schemas.insert(schema);
				return;
			}
			auto existing = FindGrant(tables, schema, table);
			if (existing && existing->Has(verb)) {
				throw IOException("virtual_catalog_bridge: a '%s' policy is already defined for '%s.%s'",
				                  CrossingVerbName(verb), schema, table);
			}
			// Before ATTACH, RedeemBridgeAttach checks this once the grant set is complete.
			if (live && (verb == CrossingVerb::UPDATE || verb == CrossingVerb::DELETE_) &&
			    !(existing && existing->Has(CrossingVerb::SELECT))) {
				throw PermissionException(
				    "virtual_catalog_bridge: table '%s.%s' in bridge '%s' grants '%s' without 'select'; UPDATE and "
				    "DELETE are driven by a scan of the table and cannot be granted alone",
				    schema, table, bridge_id, CrossingVerbName(verb));
			}
			auto &grant = tables[schema][table];
			grant.Allow(verb);
			if (grant.key.empty() && !is_view) {
				grant.key = std::move(discovered_key);
			}
			grant.using_predicates[static_cast<idx_t>(verb)] = std::move(using_predicate);
			grant.check_predicates[static_cast<idx_t>(verb)] = std::move(check_predicate);
		});
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

string RequiredAttachOption(AttachInfo &info, const char *key) {
	auto it = info.options.find(key);
	if (it == info.options.end()) {
		throw IOException("virtual_catalog_bridge: ATTACH (TYPE %s) requires a '%s' option",
		                  VIRTUAL_CATALOG_BRIDGE_TYPE, key);
	}
	auto text = StringValue::Get(it->second.DefaultCastAs(LogicalType::VARCHAR));
	if (text.empty()) {
		throw IOException("virtual_catalog_bridge: the '%s' ATTACH option cannot be empty", key);
	}
	return text;
}

void PrimaryKeyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	args.data[1].Flatten(count);
	ThrowIfAnyNull(args, 2, "bridge_primary_key");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto table_names = FlatVector::GetData<string_t>(args.data[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		string schema;
		string table;
		SplitGrantName(table_names[i].GetString(), schema, table);

		auto list = args.data[2].GetValue(i);
		if (list.IsNull()) {
			throw IOException("virtual_catalog_bridge: the key column list cannot be NULL");
		}
		vector<string> key;
		for (auto &entry : ListValue::GetChildren(list)) {
			if (entry.IsNull()) {
				throw IOException("virtual_catalog_bridge: a key column name cannot be NULL");
			}
			key.push_back(entry.ToString());
		}
		if (key.empty()) {
			throw IOException("virtual_catalog_bridge: the key column list cannot be empty");
		}

		shared_ptr<DatabaseInstance> source_db;
		SourcePin source;
		{
			lock_guard<mutex> lock(PendingMutex());
			auto target = LookupOpenGrants(bridge_id, context);
			source_db = target.source_db;
			source = target.source;
		}

		{
			Connection source_conn(*source_db);
			source_conn.BeginTransaction();
			try {
				auto catalog = source.Use(*source_conn.context);
				EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
				auto entry =
				    Catalog::GetEntry(*source_conn.context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
				if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
					throw CatalogException("virtual_catalog_bridge: '%s.%s' not found in catalog '%s' on source",
					                       schema, table, catalog);
				}
				auto &columns = entry->Cast<TableCatalogEntry>().GetColumns();
				for (auto &key_column : key) {
					if (!columns.ColumnExists(key_column)) {
						throw IOException("virtual_catalog_bridge: '%s' is not a column of '%s.%s'", key_column, schema,
						                  table);
					}
				}
			} catch (...) {
				source_conn.Rollback();
				throw;
			}
			source_conn.Rollback();
		}

		WriteGrants(bridge_id, context, [&](GrantTables &grant_tables, case_insensitive_set_t &, bool live) {
			auto &grant = GrantForKey(grant_tables, schema, table, live, bridge_id);
			grant.key = std::move(key);
			grant.key_verified = false;
		});
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void PrimaryKeyQueryFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < 3; c++) {
		args.data[c].Flatten(count);
	}
	ThrowIfAnyNull(args, 3, "bridge_primary_key_query");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto tables = FlatVector::GetData<string_t>(args.data[1]);
	auto queries = FlatVector::GetData<string_t>(args.data[2]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto qualified = tables[i].GetString();
		string schema;
		string table;
		SplitGrantName(qualified, schema, table);
		{
			lock_guard<mutex> lock(PendingMutex());
			LookupOpenGrants(bridge_id, context);
		}

		Connection source_conn(*context.db);
		auto key_result = source_conn.SendQuery(queries[i].GetString());
		if (key_result->HasError()) {
			key_result->GetErrorObject().Throw("virtual_catalog_bridge: primary key query for table '" + qualified +
			                                   "' failed: ");
		}
		vector<string> key;
		while (true) {
			auto chunk = key_result->Fetch();
			if (key_result->HasError()) {
				key_result->GetErrorObject().Throw("virtual_catalog_bridge: primary key query for table '" + qualified +
				                                   "' failed: ");
			}
			if (!chunk || chunk->size() == 0) {
				break;
			}
			for (idx_t row = 0; row < chunk->size(); row++) {
				auto value = chunk->GetValue(0, row);
				if (value.IsNull() || value.ToString().empty()) {
					throw IOException(
					    "virtual_catalog_bridge: primary key query for table '%s' returned an empty or NULL "
					    "column name",
					    qualified);
				}
				key.push_back(value.ToString());
			}
		}
		if (key.empty()) {
			throw IOException("virtual_catalog_bridge: primary key query for table '%s' returned no columns",
			                  qualified);
		}

		WriteGrants(bridge_id, context, [&](GrantTables &grant_tables, case_insensitive_set_t &, bool live) {
			auto &grant = GrantForKey(grant_tables, schema, table, live, bridge_id);
			grant.key = std::move(key);
			grant.key_verified = false;
		});
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void PrimaryKeyCheckFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < 3; c++) {
		args.data[c].Flatten(count);
	}
	ThrowIfAnyNull(args, 3, "bridge_primary_key_check");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto tables = FlatVector::GetData<string_t>(args.data[1]);
	auto queries = FlatVector::GetData<string_t>(args.data[2]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		auto qualified = tables[i].GetString();
		string schema;
		string table;
		SplitGrantName(qualified, schema, table);
		ReadGrants(bridge_id, context, [&](GrantTables &grant_tables, case_insensitive_set_t &, bool live) {
			if (GrantForKey(grant_tables, schema, table, live, bridge_id).key.empty()) {
				throw IOException("virtual_catalog_bridge: no primary key is set for '%s' in bridge '%s'; call "
				                  "bridge_primary_key or bridge_primary_key_query first",
				                  qualified, bridge_id);
			}
		});

		Connection source_conn(*context.db);
		auto check_result = source_conn.SendQuery(queries[i].GetString());
		if (check_result->HasError()) {
			check_result->GetErrorObject().Throw("virtual_catalog_bridge: primary key check for table '" + qualified +
			                                     "' failed: ");
		}
		auto chunk = check_result->Fetch();
		if (check_result->HasError()) {
			check_result->GetErrorObject().Throw("virtual_catalog_bridge: primary key check for table '" + qualified +
			                                     "' failed: ");
		}
		if (chunk && chunk->size() > 0) {
			throw ConstraintException("virtual_catalog_bridge: primary key check for table '%s' returned rows; the "
			                          "declared key is not unique on the source",
			                          qualified);
		}

		WriteGrants(bridge_id, context, [&](GrantTables &grant_tables, case_insensitive_set_t &, bool live) {
			GrantForKey(grant_tables, schema, table, live, bridge_id).key_verified = true;
		});
		result_data[i] = StringVector::AddString(result, "ok");
	}
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
ContextTarget LookupContext(const string &bridge_id, ClientContext &context, const string &name) {
	ContextTarget target;
	PurgeExpiredPendingSources();
	if (PendingSources().count(bridge_id)) {
		auto &pending = LookupPendingForGrant(bridge_id, context);
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
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto id_value = args.data[0].GetValue(i);
		auto name_value = args.data[1].GetValue(i);
		auto value = args.data[2].GetValue(i);
		if (id_value.IsNull() || name_value.IsNull()) {
			throw IOException("virtual_catalog_bridge: bridge_set_context needs a bridge id and a context name");
		}
		auto bridge_id = StringValue::Get(id_value);
		auto name = StringValue::Get(name_value);
		if (name.empty()) {
			throw IOException("virtual_catalog_bridge: the context name cannot be empty");
		}
		if (value.IsNull()) {
			throw IOException("virtual_catalog_bridge: context '%s' cannot be NULL", name);
		}
		ContextTarget target;
		{
			lock_guard<mutex> lock(PendingMutex());
			target = LookupContext(bridge_id, context, name);
		}
		auto values = ContextSnapshot(*target.context);
		if (target.context->finalized) {
			throw PermissionException("virtual_catalog_bridge: the context of bridge '%s' is finalized", bridge_id);
		}
		values[name] = value;
		if (!target.dependents.empty()) {
			Connection source_conn(*context.db);
			source_conn.BeginTransaction();
			try {
				auto catalog = target.source.Use(*source_conn.context);
				for (auto &dependent : target.dependents) {
					EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, dependent.table);
					auto entry = Catalog::GetEntry(*source_conn.context, catalog, dependent.schema, lookup,
					                               OnEntryNotFound::RETURN_NULL);
					if (!IsGrantable(entry)) {
						continue;
					}
					auto source_table = SourceRelation(source_conn, *entry, catalog, dependent.schema, dependent.table);
					ProbePredicate(source_table, *dependent.predicate, values, dependent.predicate->ToString(),
					               dependent.clause);
				}
			} catch (...) {
				source_conn.Rollback();
				throw;
			}
			source_conn.Rollback();
		}
		lock_guard<mutex> guard(target.context->lock);
		if (target.context->finalized) {
			throw PermissionException("virtual_catalog_bridge: the context of bridge '%s' is finalized", bridge_id);
		}
		target.context->values[name] = std::move(value);
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void FinalizeContextFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	ThrowIfAnyNull(args, 1, "bridge_finalize_context");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		lock_guard<mutex> lock(PendingMutex());
		auto target = LookupContext(bridge_id, context, string());
		lock_guard<mutex> guard(target.context->lock);
		if (target.context->finalized) {
			throw PermissionException("virtual_catalog_bridge: the context of bridge '%s' is already finalized",
			                          bridge_id);
		}
		target.context->finalized = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void FinalizePolicyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	args.data[0].Flatten(count);
	ThrowIfAnyNull(args, 1, "bridge_finalize_policy");
	auto bridge_ids = FlatVector::GetData<string_t>(args.data[0]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		auto bridge_id = bridge_ids[i].GetString();
		lock_guard<mutex> lock(PendingMutex());
		auto target = LookupGrantTarget(bridge_id, context);
		if (*target.policy_finalized) {
			throw PermissionException("virtual_catalog_bridge: the policies of bridge '%s' are already finalized",
			                          bridge_id);
		}
		*target.policy_finalized = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

} // namespace

unique_ptr<DuckDBSource> RedeemBridgeAttach(AttachInfo &info, DatabaseInstance &target_db) {
	auto bridge_id = RequiredAttachOption(info, "id");
	auto token = RequiredAttachOption(info, "token");

	auto colon = token.find(':');
	if (colon != string::npos && token.substr(0, colon) != LoadNonce()) {
		throw PermissionException(
		    "virtual_catalog_bridge: source and target loaded different copies of the virtual_catalog extension. "
		    "Ensure both connections use the same extension installation.");
	}

	shared_ptr<DatabaseInstance> source_db;
	SourcePin source;
	auto grants = make_shared_ptr<GrantBook>();
	{
		lock_guard<mutex> lock(PendingMutex());
		PurgeExpiredPendingSources();
		auto it = PendingSources().find(bridge_id);
		if (it == PendingSources().end()) {
			throw PermissionException("virtual_catalog_bridge: no registered source for bridge '%s'", bridge_id);
		}
		if (it->second.token != token) {
			throw PermissionException("virtual_catalog_bridge: invalid setup token for bridge '%s'", bridge_id);
		}
		idx_t table_count = it->second.create_schemas.size();
		for (auto &schema_entry : it->second.schemas) {
			table_count += schema_entry.second.size();
			for (auto &table_entry : schema_entry.second) {
				auto &grant = table_entry.second;
				if (grant.Has(CrossingVerb::SELECT)) {
					continue;
				}
				for (auto verb : {CrossingVerb::UPDATE, CrossingVerb::DELETE_}) {
					if (grant.Has(verb)) {
						throw PermissionException(
						    "virtual_catalog_bridge: table '%s.%s' in bridge '%s' grants '%s' without "
						    "'select'; UPDATE and DELETE are driven by a scan of the table and cannot be "
						    "granted alone",
						    schema_entry.first, table_entry.first, bridge_id, CrossingVerbName(verb));
					}
				}
			}
		}
		if (table_count == 0) {
			throw PermissionException("virtual_catalog_bridge: bridge '%s' has no policies; call "
			                          "bridge_policy(...) on the source connection before attaching",
			                          bridge_id);
		}
		source_db = it->second.source_db;
		source = it->second.source;
		grants->tables = std::move(it->second.schemas);
		grants->create_schemas = std::move(it->second.create_schemas);
		grants->context = std::move(it->second.context);
		grants->policy_finalized = it->second.policy_finalized;
		PendingSources().erase(it);
		LiveBridges()[bridge_id] = LiveBridge {source_db.get(), source, grants};
	}
	return make_uniq<DuckDBSource>(bridge_id, source_db, target_db, std::move(source), std::move(grants));
}

SourcePin::SourcePin(AttachedDatabase &attached_p, string registered_name_p)
    : attached(attached_p.shared_from_this()), registered_name(std::move(registered_name_p)) {
}

string SourcePin::Use(ClientContext &source_context) const {
	auto pinned = attached.lock();
	if (!pinned || DatabaseManager::Get(source_context).GetDatabase(pinned->GetName()) != pinned) {
		throw CatalogException("virtual_catalog_bridge: source catalog '%s' was detached", registered_name);
	}
	auto name = pinned->GetName();
	auto &transaction = MetaTransaction::Get(source_context);
	auto referenced = transaction.GetReferencedDatabase(name);
	if (referenced && referenced.get() != pinned.get()) {
		throw CatalogException("virtual_catalog_bridge: source catalog '%s' was detached", registered_name);
	}
	transaction.UseDatabase(pinned);
	return name;
}

bool TryParseCrossingVerb(const string &text, CrossingVerb &out) {
	for (auto verb : CrossingVerbs()) {
		if (StringUtil::CIEquals(text, CrossingVerbName(verb))) {
			out = verb;
			return true;
		}
	}
	return false;
}

DuckDBSource::DuckDBSource(string bridge_id_p, const shared_ptr<DatabaseInstance> &source_db_p,
                           DatabaseInstance &target_db, SourcePin source_p, shared_ptr<GrantBook> grants_p)
    : bridge_id(std::move(bridge_id_p)), source_db(source_db_p),
      source_db_hold(source_db_p.get() == &target_db ? nullptr : source_db_p), source(std::move(source_p)),
      grants(std::move(grants_p)) {
}

shared_ptr<DatabaseInstance> DuckDBSource::Database() const {
	auto db = source_db.lock();
	if (!db) {
		throw ConnectionException("virtual_catalog_bridge: the source database of bridge '%s' is closed", bridge_id);
	}
	return db;
}

DuckDBSource::~DuckDBSource() {
	lock_guard<mutex> lock(PendingMutex());
	auto it = LiveBridges().find(bridge_id);
	if (it != LiveBridges().end() && it->second.grants.lock() == grants) {
		LiveBridges().erase(it);
	}
}

void DuckDBSource::SetTarget(optional_ptr<CrossingAttach> target) {
	lock_guard<mutex> guard(grants->lock);
	grants->target = target;
}

vector<string> DuckDBSource::Schemas() {
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
	lock_guard<mutex> guard(grants->lock);
	CrossingSchema described;
	described.name = schema;
	if (grants->create_schemas.count(schema)) {
		described.verbs.push_back(CrossingVerb::CREATE);
	}
	return described;
}

CrossingTable DuckDBSource::Describe(const string &schema, const string &name) {
	uint8_t verbs;
	vector<string> key;
	bool key_verified;
	{
		lock_guard<mutex> guard(grants->lock);
		auto schema_it = grants->tables.find(schema);
		if (schema_it == grants->tables.end()) {
			throw CatalogException("virtual_catalog_bridge: nothing is granted in schema '%s'", schema);
		}
		auto table_it = schema_it->second.find(name);
		if (table_it == schema_it->second.end()) {
			throw CatalogException("virtual_catalog_bridge: '%s.%s' is not granted", schema, name);
		}
		verbs = table_it->second.verbs;
		key = table_it->second.key;
		key_verified = table_it->second.key_verified;
	}

	CrossingTable table;
	table.name = name;
	const auto describe = [&](ClientContext &context) {
		auto catalog = source.Use(context);
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
		auto entry = Catalog::GetEntry(context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
		if (!IsGrantable(entry)) {
			throw CatalogException("virtual_catalog_bridge: '%s.%s' is no longer on the source", schema, name);
		}
		if (entry->type == CatalogType::VIEW_ENTRY) {
			auto &view = entry->Cast<ViewCatalogEntry>();
			view.BindView(context);
			auto columns = view.GetColumnInfo();
			auto names = BindContext::AliasColumnNames(name, columns->names, view.aliases);
			for (idx_t i = 0; i < names.size(); i++) {
				table.Column(names[i], columns->types[i]);
			}
			return;
		}
		auto &source_table = entry->Cast<TableCatalogEntry>();
		for (auto &column : source_table.GetColumns().Logical()) {
			table.Column(column.GetName(), column.GetType());
		}
		for (auto &constraint : source_table.GetConstraints()) {
			if (constraint->type == ConstraintType::FOREIGN_KEY) {
				continue;
			}
			table.constraints.push_back(constraint->Copy());
		}
	};

	shared_ptr<Connection> shaping;
	{
		lock_guard<mutex> guard(grants->lock);
		auto it = grants->shaping.find(schema + "." + name);
		if (it != grants->shaping.end()) {
			shaping = it->second;
		}
	}
	if (shaping) {
		describe(*shaping->context);
	} else {
		Connection conn(*Database());
		conn.BeginTransaction();
		try {
			describe(*conn.context);
		} catch (...) {
			conn.Rollback();
			throw;
		}
		conn.Rollback();
	}

	for (auto verb : CrossingVerbs()) {
		if ((verbs & static_cast<uint8_t>(1u << static_cast<uint8_t>(verb))) != 0) {
			table.verbs.push_back(verb);
		}
	}
	table.key = std::move(key);
	table.key_unique = !table.key.empty() && key_verified;
	return table;
}

DuckDBSession::DuckDBSession(shared_ptr<DatabaseInstance> source_db_p, SourcePin source_p,
                             shared_ptr<GrantBook> grants_p, bool autocommit_p)
    : source_db(std::move(source_db_p)), source(std::move(source_p)), grants(std::move(grants_p)),
      autocommit(autocommit_p) {
}

shared_ptr<Connection> DuckDBSession::Shared() {
	lock_guard<mutex> guard(lock);
	if (!conn) {
		conn = make_shared_ptr<Connection>(*source_db);
		conn->BeginTransaction();
	}
	return conn;
}

void DuckDBSession::ForgetShaping() {
	if (!conn) {
		return;
	}
	for (auto it = grants->shaping.begin(); it != grants->shaping.end();) {
		it = it->second == conn ? grants->shaping.erase(it) : std::next(it);
	}
}

void DuckDBSession::UndoGrants() {
	for (auto it = grant_undo.rbegin(); it != grant_undo.rend(); ++it) {
		(*it)();
	}
	grant_undo.clear();
}

void DuckDBSession::Commit() {
	lock_guard<mutex> guard(lock);
	if (!conn) {
		return;
	}
	try {
		conn->Commit();
	} catch (...) {
		lock_guard<mutex> grants_guard(grants->lock);
		UndoGrants();
		ForgetShaping();
		conn.reset();
		throw;
	}
	lock_guard<mutex> grants_guard(grants->lock);
	grant_undo.clear();
	ForgetShaping();
	conn.reset();
}

void DuckDBSession::Rollback() {
	lock_guard<mutex> guard(lock);
	{
		lock_guard<mutex> grants_guard(grants->lock);
		UndoGrants();
		ForgetShaping();
	}
	if (!conn) {
		return;
	}
	try {
		conn->Rollback();
	} catch (...) { // NOLINT: the target's transaction is already resolving
	}
	conn.reset();
}

unique_ptr<DuckDBSession> DuckDBSource::Begin(ClientContext &context) {
	auto autocommit = context.transaction.IsAutoCommit() ||
	                  context.registered_state->Get<StreamingReadMarker>(StreamingReadMarkerKey()) != nullptr;
	return make_uniq<DuckDBSession>(Database(), source, grants, autocommit);
}

shared_ptr<Connection> DuckDBSource::PlanningConnection() {
	auto planning = make_shared_ptr<Connection>(*Database());
	planning->BeginTransaction();
	return planning;
}

CrossingPlan DuckDBSource::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		auto &described = *request.described;
		return CrossingPlan::Of(
		    MakeFloorNode(request.schema, request.table, described.column_names, described.column_types));
	}
	return CrossingPlan::Of(MakeSeamNode(request.seam.types));
}

bool DuckDBSource::SourceHasFunction(const string &function_name) {
	{
		lock_guard<mutex> guard(functions_lock);
		auto it = functions_known.find(function_name);
		if (it != functions_known.end()) {
			return it->second;
		}
	}
	auto planning_conn = PlanningConnection();
	auto &context = *planning_conn->context;
	EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function_name);
	EntryLookupInfo aggregate_lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, function_name);
	bool known =
	    Catalog::GetEntry(context, INVALID_CATALOG, DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL) ||
	    Catalog::GetEntry(context, INVALID_CATALOG, DEFAULT_SCHEMA, aggregate_lookup, OnEntryNotFound::RETURN_NULL);
	lock_guard<mutex> guard(functions_lock);
	functions_known[function_name] = known;
	return known;
}

CrossingVerdict DuckDBSource::AcceptsCall(const Expression &expr) {
	string function_name;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		function_name = expr.Cast<BoundFunctionExpression>().function.name;
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		function_name = expr.Cast<BoundAggregateExpression>().function.name;
		break;
	default:
		return CrossingVerdict::Yes();
	}
	if (SourceHasFunction(function_name)) {
		return CrossingVerdict::Yes();
	}
	return CrossingVerdict::No("the source has no function " + function_name);
}

CrossingVerdict DuckDBSource::AcceptsType(const LogicalType &type) {
	return CrossingVerdict::Yes();
}

CrossingWriter DuckDBSession::Write(ClientContext &, const CrossingQuery &query) {
	return [this, &query](ClientContext &, const CrossingWaker &) {
		auto write_conn = Shared();
		auto &context = *write_conn->context;
		auto catalog = source.Use(context);
		CrossingWriteTarget target;
		target.catalog = catalog;
		target.schema = query.written.schema;
		target.table = query.written.table;
		target.verb = query.kind;
		target.key_columns = query.key_columns;
		target.set_columns = query.set_columns;
		auto shape = PolicyShaper(grants, target);
		if (auto rows = SeamRowsOf(query.plan)) {
			return CrossingWriteResult::Done(ExecuteWrite(context, target, SeamRefOfRows(*rows), shape));
		}
		auto plan = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
		BindFloors(context, plan, catalog, PolicyResolver(grants, catalog));
		return CrossingWriteResult::Done(ExecuteWrite(context, target, std::move(plan), shape));
	};
}

void DuckDBSession::Ddl(ClientContext &, const CrossingDdl &ddl) {
	auto ddl_conn = Shared();
	auto &source_conn = *ddl_conn;
	auto catalog = source.Use(*source_conn.context);
	unique_ptr<SQLStatement> statement;
	idx_t modification;
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		auto create = make_uniq<CreateStatement>();
		create->info = ddl.create->Copy();
		create->info->catalog = catalog;
		statement = std::move(create);
		modification = DatabaseModificationType::CREATE_CATALOG_ENTRY;
		break;
	}
	case CrossingVerb::ALTER: {
		auto alter = make_uniq<AlterStatement>();
		alter->info = ddl.alter->Copy();
		alter->info->catalog = catalog;
		statement = std::move(alter);
		modification = DatabaseModificationType::ALTER_TABLE;
		break;
	}
	case CrossingVerb::DROP: {
		auto drop = make_uniq<DropStatement>();
		drop->info = ddl.drop->Copy();
		drop->info->catalog = catalog;
		statement = std::move(drop);
		modification = DatabaseModificationType::DROP_CATALOG_ENTRY;
		break;
	}
	default:
		throw InternalException("virtual_catalog_bridge: '%s' is not a DDL verb", CrossingVerbName(ddl.verb));
	}

	auto creates = ddl.verb != CrossingVerb::CREATE || CreateWouldCreate(source_conn, catalog, ddl);
	if (ddl.verb == CrossingVerb::ALTER) {
		ThrowIfAlterOrphansPolicy(ddl);
	}
	auto &attached = Catalog::GetCatalog(*source_conn.context, catalog).GetAttached();
	MetaTransaction::Get(*source_conn.context).ModifyDatabase(attached, modification);

	auto pending = source_conn.context->PendingQuery(std::move(statement), QueryParameters(false));
	if (pending->HasError()) {
		pending->GetErrorObject().Throw("virtual_catalog_bridge: ddl on source failed: ");
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_bridge: ddl on source failed: ");
	}
	if (creates) {
		RecordDdl(ddl_conn, catalog, ddl);
	}
}

bool DuckDBSession::CreateWouldCreate(Connection &source_conn, const string &catalog, const CrossingDdl &ddl) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, ddl.table);
	auto existing = Catalog::GetEntry(*source_conn.context, catalog, ddl.schema, lookup, OnEntryNotFound::RETURN_NULL);
	if (!existing) {
		return true;
	}
	switch (ddl.create->on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return false;
	case OnCreateConflict::REPLACE_ON_CONFLICT: {
		lock_guard<mutex> guard(grants->lock);
		auto schema_it = grants->tables.find(ddl.schema);
		auto granted = schema_it != grants->tables.end() && schema_it->second.count(ddl.table);
		if (!granted) {
			throw PermissionException("virtual_catalog_bridge: '%s' does not have '%s' permission", ddl.table,
			                          CrossingVerbName(CrossingVerb::DROP));
		}
		return true;
	}
	default:
		return true;
	}
}

void DuckDBSession::ThrowIfAlterOrphansPolicy(const CrossingDdl &ddl) {
	if (ddl.alter->type != AlterType::ALTER_TABLE) {
		return;
	}
	auto &alter = ddl.alter->Cast<AlterTableInfo>();
	string column;
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_COLUMN:
		column = alter.Cast<RenameColumnInfo>().old_name;
		break;
	case AlterTableType::REMOVE_COLUMN:
		column = alter.Cast<RemoveColumnInfo>().removed_column;
		break;
	case AlterTableType::ALTER_COLUMN_TYPE:
		column = alter.Cast<ChangeColumnTypeInfo>().column_name;
		break;
	default:
		return;
	}
	lock_guard<mutex> guard(grants->lock);
	auto schema_it = grants->tables.find(ddl.schema);
	if (schema_it == grants->tables.end()) {
		return;
	}
	auto table_it = schema_it->second.find(ddl.table);
	if (table_it == schema_it->second.end()) {
		return;
	}
	auto &grant = table_it->second;
	bool orphaned = false;
	for (auto &key_column : grant.key) {
		orphaned = orphaned || StringUtil::CIEquals(key_column, column);
	}
	const auto mentions = [&](const ParsedExpression &expr) {
		bool found = false;
		std::function<void(const ParsedExpression &)> visit = [&](const ParsedExpression &node) {
			if (node.GetExpressionClass() == ExpressionClass::COLUMN_REF &&
			    StringUtil::CIEquals(node.Cast<ColumnRefExpression>().GetColumnName(), column)) {
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
		throw IOException("virtual_catalog_bridge: '%s' is the key or a policy column of '%s.%s'", column, ddl.schema,
		                  ddl.table);
	}
}

void DuckDBSession::RecordDdl(const shared_ptr<Connection> &source_conn, const string &catalog,
                              const CrossingDdl &ddl) {
	lock_guard<mutex> guard(grants->lock);
	auto book = grants;
	auto schema_name = ddl.schema;
	auto table_name = ddl.table;
	auto &schema = book->tables[schema_name];
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		SourceGrant grant;
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table_name);
		auto entry =
		    Catalog::GetEntry(*source_conn->context, catalog, schema_name, lookup, OnEntryNotFound::RETURN_NULL);
		if (entry && entry->type == CatalogType::TABLE_ENTRY) {
			grant.key = DiscoverKeyColumns(entry->Cast<TableCatalogEntry>());
		}
		for (auto verb : CrossingVerbs()) {
			if ((verb == CrossingVerb::UPDATE || verb == CrossingVerb::DELETE_) && grant.key.empty()) {
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
		book->shaping[schema_name + "." + table_name] = source_conn;
		grant_undo.emplace_back([book, schema_name, table_name, replaced]() {
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
		grant_undo.emplace_back([book, schema_name, table_name, dropped]() {
			book->tables[schema_name][table_name] = std::move(*dropped);
		});
		return;
	}
	default:
		break;
	}
	book->shaping[schema_name + "." + table_name] = source_conn;
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
	auto new_name = alter.Cast<RenameTableInfo>().new_table_name;
	auto moved = std::move(it->second);
	schema.erase(it);
	schema[new_name] = std::move(moved);
	book->shaping[schema_name + "." + new_name] = source_conn;
	grant_undo.emplace_back([book, schema_name, table_name, new_name]() {
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

namespace {

unique_ptr<QueryResult> RunRead(Connection &source_conn, const CrossingQuery &query, bool stream,
                                const CrossingFloorResolver &resolver, const string &source_catalog) {
	auto &context = *source_conn.context;
	auto plan = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
	BindFloors(context, plan, source_catalog, resolver);

	auto pending = PendingCrossingPlan(context, std::move(plan), stream);
	if (pending->HasError()) {
		pending->GetErrorObject().Throw("virtual_catalog_bridge: plan on source failed: ");
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_bridge: plan on source failed: ");
	}
	return result;
}

CrossingScan ScanOf(const shared_ptr<Connection> &conn, unique_ptr<QueryResult> owned) {
	shared_ptr<QueryResult> result(std::move(owned));
	CrossingScan scan;
	scan.open = [conn, result](ClientContext &, idx_t) -> CrossingReader {
		return [conn, result](ClientContext &, DataChunk &chunk, const CrossingWaker &) {
			auto raw = result->FetchRaw();
			if (result->HasError()) {
				result->ThrowError("virtual_catalog_bridge: source query failed: ");
			}
			if (!raw || raw->size() == 0) {
				return CrossingReadResult::Done();
			}
			chunk.Reference(*raw);
			return CrossingReadResult::Rows();
		};
	};
	return scan;
}

} // namespace

CrossingScan DuckDBSession::Read(ClientContext &, const CrossingQuery &query) {
	if (autocommit) {
		auto read_conn = make_shared_ptr<Connection>(*source_db);
		read_conn->context->registered_state->Insert(StreamingReadMarkerKey(), make_shared_ptr<StreamingReadMarker>());
		read_conn->BeginTransaction();
		auto catalog = source.Use(*read_conn->context);
		return ScanOf(read_conn, RunRead(*read_conn, query, true, PolicyResolver(grants, catalog), catalog));
	}
	auto shared = Shared();
	auto catalog = source.Use(*shared->context);
	lock_guard<mutex> guard(lock);
	return ScanOf(shared, RunRead(*shared, query, false, PolicyResolver(grants, catalog), catalog));
}

void RegisterBridgeFunctions(ExtensionLoader &loader) {
	ScalarFunctionSet register_source_set("bridge_register_source");
	ScalarFunction register_source({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               RegisterSourceFunc);
	register_source.stability = FunctionStability::VOLATILE;
	register_source.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	register_source_set.AddFunction(register_source);
	ScalarFunction register_source_token({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                     LogicalType::VARCHAR, RegisterSourceFunc);
	register_source_token.stability = FunctionStability::VOLATILE;
	register_source_token.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	register_source_set.AddFunction(register_source_token);
	loader.RegisterFunction(register_source_set);

	ScalarFunctionSet policy_set("bridge_policy");
	ScalarFunction policy_using(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    PolicyFunc);
	policy_using.stability = FunctionStability::VOLATILE;
	policy_using.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	policy_set.AddFunction(policy_using);
	ScalarFunction policy_check(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, PolicyFunc);
	policy_check.stability = FunctionStability::VOLATILE;
	policy_check.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	policy_set.AddFunction(policy_check);
	loader.RegisterFunction(policy_set);

	ScalarFunction primary_key("bridge_primary_key",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	                           LogicalType::VARCHAR, PrimaryKeyFunc);
	primary_key.stability = FunctionStability::VOLATILE;
	primary_key.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(primary_key);

	ScalarFunction primary_key_query("bridge_primary_key_query",
	                                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, PrimaryKeyQueryFunc);
	primary_key_query.stability = FunctionStability::VOLATILE;
	primary_key_query.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(primary_key_query);

	ScalarFunction primary_key_check("bridge_primary_key_check",
	                                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, PrimaryKeyCheckFunc);
	primary_key_check.stability = FunctionStability::VOLATILE;
	primary_key_check.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(primary_key_check);

	ScalarFunction set_context("bridge_set_context", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
	                           LogicalType::VARCHAR, SetContextFunc);
	set_context.stability = FunctionStability::VOLATILE;
	set_context.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(set_context);

	ScalarFunction finalize_context("bridge_finalize_context", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                FinalizeContextFunc);
	finalize_context.stability = FunctionStability::VOLATILE;
	finalize_context.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(finalize_context);

	ScalarFunction finalize_policy("bridge_finalize_policy", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               FinalizePolicyFunc);
	finalize_policy.stability = FunctionStability::VOLATILE;
	finalize_policy.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(finalize_policy);
}

} // namespace duckdb
