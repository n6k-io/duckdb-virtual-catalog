#include "duckdb_source.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
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
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

struct PendingV2Source {
	shared_ptr<DatabaseInstance> source_db;
	string token;
	string source_catalog;
	std::chrono::steady_clock::time_point last_touched;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> schemas;
	case_insensitive_set_t create_schemas;
};

constexpr int64_t PENDING_SOURCE_TTL_SECONDS = 30;

const string &LoadNonce() {
	static const string nonce = UUID::ToString(UUID::GenerateRandomUUID());
	return nonce;
}

mutex &PendingMutex() {
	static auto *lock = new mutex();
	return *lock;
}

unordered_map<string, PendingV2Source> &PendingSources() {
	static auto *pending = new unordered_map<string, PendingV2Source>();
	return *pending;
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

PendingV2Source &LookupPendingForGrant(const string &bridge_id, ClientContext &context) {
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
	auto source_entry = Catalog::GetCatalogEntry(context, it->second.source_catalog);
	if (source_entry && source_entry->GetCatalogType() == VIRTUAL_CATALOG_BRIDGE_TYPE) {
		throw CatalogException("virtual_catalog_bridge: source catalog '%s' is itself a bridge; name the "
		                       "catalog holding the real tables",
		                       it->second.source_catalog);
	}
	it->second.last_touched = std::chrono::steady_clock::now();
	return it->second;
}

void RegisterSourceFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	auto arg_count = args.ColumnCount();
	for (idx_t c = 0; c < arg_count; c++) {
		args.data[c].Flatten(count);
	}
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
		if (entry->GetCatalogType() == VIRTUAL_CATALOG_BRIDGE_TYPE) {
			throw CatalogException("virtual_catalog_bridge: source catalog '%s' is itself a bridge; name the "
			                       "catalog holding the real tables",
			                       source_catalog);
		}

		PendingV2Source pending;
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
		pending.source_catalog = source_catalog;
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

bool ExpressionContainsParameter(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::PARAMETER) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { found = found || ExpressionContainsParameter(child); });
	return found;
}

unique_ptr<ParsedExpression> ParseAndValidatePredicate(const shared_ptr<Relation> &source_table, const string &text,
                                                       const char *clause) {
	auto parsed = Parser::ParseExpressionList(text);
	if (parsed.size() != 1) {
		throw IOException("virtual_catalog_bridge: the %s predicate must be a single expression", clause);
	}
	auto expr = std::move(parsed[0]);
	if (ExpressionContainsParameter(*expr)) {
		throw IOException("virtual_catalog_bridge: the %s predicate '%s' may not contain parameters", clause, text);
	}
	ClearQueryLocations(*expr);

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

void PointCheckAtUpdatedRow(ParsedExpression &expr, const string &table, const vector<string> &set_columns,
                            const CrossingWriteStatement &built, idx_t key_count) {
	ParsedExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
		if (child->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &column_ref = child->Cast<ColumnRefExpression>();
			if (!column_ref.IsQualified()) {
				auto &name = column_ref.GetColumnName();
				for (idx_t c = 0; c < set_columns.size(); c++) {
					if (StringUtil::CIEquals(set_columns[c], name)) {
						child = make_uniq<ColumnRefExpression>(built.seam_columns[key_count + c], built.seam_alias);
						return;
					}
				}
				child = make_uniq<ColumnRefExpression>(name, table);
				return;
			}
		}
		PointCheckAtUpdatedRow(*child, table, set_columns, built, key_count);
	});
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
	return [grants, source_catalog](const CrossingFloor &floor) -> unique_ptr<TableRef> {
		auto using_predicate = UsingFor(*grants, floor.schema, floor.table, CrossingVerb::SELECT);
		if (!using_predicate) {
			return nullptr;
		}
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
	return [grants, target](CrossingWriteStatement &built) {
		auto using_predicate = UsingFor(*grants, target.schema, target.table, target.verb);
		auto check = CheckFor(*grants, target.schema, target.table, target.verb);
		switch (target.verb) {
		case CrossingVerb::INSERT: {
			if (!check) {
				return;
			}
			auto &select = built.statement->Cast<InsertStatement>().select_statement->node->Cast<SelectNode>();
			PointCheckAtUpdatedRow(*check, target.table, target.set_columns, built, 0);
			select.select_list[0] = GuardWithCheck(std::move(check), std::move(select.select_list[0]));
			return;
		}
		case CrossingVerb::UPDATE: {
			auto &set_info = *built.statement->Cast<UpdateStatement>().set_info;
			if (using_predicate) {
				set_info.condition = And(std::move(set_info.condition), std::move(using_predicate));
			}
			if (check) {
				PointCheckAtUpdatedRow(*check, target.table, target.set_columns, built, target.key_columns.size());
				set_info.expressions[0] = GuardWithCheck(std::move(check), std::move(set_info.expressions[0]));
			}
			return;
		}
		case CrossingVerb::DELETE_: {
			auto &statement = built.statement->Cast<DeleteStatement>();
			if (using_predicate) {
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

		lock_guard<mutex> lock(PendingMutex());
		auto &pending = LookupPendingForGrant(bridge_id, context);

		Connection source_conn(*pending.source_db);
		source_conn.BeginTransaction();
		try {
			if (verb == CrossingVerb::CREATE) {
				EntryLookupInfo lookup(CatalogType::SCHEMA_ENTRY, schema);
				auto entry = Catalog::GetSchema(*source_conn.context, pending.source_catalog, lookup,
				                                OnEntryNotFound::RETURN_NULL);
				if (!entry) {
					throw CatalogException("virtual_catalog_bridge: schema '%s' not found in catalog '%s' on source",
					                       schema, pending.source_catalog);
				}
				if (pending.create_schemas.count(schema)) {
					throw IOException("virtual_catalog_bridge: a 'create' policy is already defined for schema '%s'",
					                  schema);
				}
				pending.create_schemas.insert(schema);
				source_conn.Rollback();
				result_data[i] = StringVector::AddString(result, "ok");
				continue;
			}

			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
			auto entry = Catalog::GetEntry(*source_conn.context, pending.source_catalog, schema, lookup,
			                               OnEntryNotFound::RETURN_NULL);
			if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
				throw CatalogException("virtual_catalog_bridge: '%s.%s' not found in catalog '%s' on source", schema,
				                       table, pending.source_catalog);
			}

			auto source_table = source_conn.Table(pending.source_catalog, schema, table);

			auto using_predicate = ParseAndValidatePredicate(source_table, using_text, "USING");
			unique_ptr<ParsedExpression> check_predicate;
			if (has_check) {
				auto checks = FlatVector::GetData<string_t>(args.data[4]);
				check_predicate = ParseAndValidatePredicate(source_table, checks[i].GetString(), "WITH CHECK");
			} else if (verb == CrossingVerb::INSERT) {
				throw IOException("virtual_catalog_bridge: an 'insert' grant must state a WITH CHECK predicate");
			} else if (verb == CrossingVerb::UPDATE && using_predicate) {
				check_predicate = using_predicate->Copy();
			}
			if (IsDdlVerb(verb)) {
				using_predicate.reset();
			}

			auto &grant = pending.schemas[schema][table];
			if (grant.Has(verb)) {
				throw IOException("virtual_catalog_bridge: a '%s' policy is already defined for '%s.%s'",
				                  CrossingVerbName(verb), schema, table);
			}
			grant.Allow(verb);
			if (grant.key.empty()) {
				grant.key = DiscoverKeyColumns(entry->Cast<TableCatalogEntry>());
			}
			grant.using_predicates[static_cast<idx_t>(verb)] = std::move(using_predicate);
			grant.check_predicates[static_cast<idx_t>(verb)] = std::move(check_predicate);
		} catch (...) {
			source_conn.Rollback();
			throw;
		}
		source_conn.Rollback();
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

		lock_guard<mutex> lock(PendingMutex());
		auto &pending = LookupPendingForGrant(bridge_id, context);

		Connection source_conn(*pending.source_db);
		source_conn.BeginTransaction();
		try {
			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
			auto entry = Catalog::GetEntry(*source_conn.context, pending.source_catalog, schema, lookup,
			                               OnEntryNotFound::RETURN_NULL);
			if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
				throw CatalogException("virtual_catalog_bridge: '%s.%s' not found in catalog '%s' on source", schema,
				                       table, pending.source_catalog);
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

		auto &grant = pending.schemas[schema][table];
		grant.key = std::move(key);
		grant.key_verified = false;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void PrimaryKeyQueryFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < 3; c++) {
		args.data[c].Flatten(count);
	}
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
			LookupPendingForGrant(bridge_id, context);
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

		lock_guard<mutex> lock(PendingMutex());
		auto &pending = LookupPendingForGrant(bridge_id, context);
		auto &grant = pending.schemas[schema][table];
		grant.key = std::move(key);
		grant.key_verified = false;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

void PrimaryKeyCheckFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto count = args.size();
	for (idx_t c = 0; c < 3; c++) {
		args.data[c].Flatten(count);
	}
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
			auto &pending = LookupPendingForGrant(bridge_id, context);
			if (pending.schemas[schema][table].key.empty()) {
				throw IOException("virtual_catalog_bridge: no primary key is set for '%s' in bridge '%s'; call "
				                  "bridge_primary_key or bridge_primary_key_query first",
				                  qualified, bridge_id);
			}
		}

		Connection source_conn(*context.db);
		auto check_result = source_conn.SendQuery(queries[i].GetString());
		if (check_result->HasError()) {
			check_result->GetErrorObject().Throw("virtual_catalog_bridge: primary key check for table '" + qualified +
			                                     "' failed: ");
		}
		auto chunk = check_result->Fetch();
		if (chunk && chunk->size() > 0) {
			throw ConstraintException("virtual_catalog_bridge: primary key check for table '%s' returned rows; the "
			                          "declared key is not unique on the source",
			                          qualified);
		}

		lock_guard<mutex> lock(PendingMutex());
		auto &pending = LookupPendingForGrant(bridge_id, context);
		pending.schemas[schema][table].key_verified = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

} // namespace

unique_ptr<DuckDBSource> RedeemBridgeAttach(ClientContext &, AttachInfo &info) {
	auto bridge_id = RequiredAttachOption(info, "id");
	auto token = RequiredAttachOption(info, "token");

	auto colon = token.find(':');
	if (colon != string::npos && token.substr(0, colon) != LoadNonce()) {
		throw PermissionException(
		    "virtual_catalog_bridge: source and target loaded different copies of the virtual_catalog extension. "
		    "Ensure both connections use the same extension installation.");
	}

	shared_ptr<DatabaseInstance> source_db;
	string source_catalog;
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
		source_catalog = std::move(it->second.source_catalog);
		grants->tables = std::move(it->second.schemas);
		grants->create_schemas = std::move(it->second.create_schemas);
		PendingSources().erase(it);
	}
	return make_uniq<DuckDBSource>(source_db, std::move(source_catalog), std::move(grants));
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

DuckDBSource::DuckDBSource(shared_ptr<DatabaseInstance> source_db_p, string source_catalog_p,
                           shared_ptr<GrantBook> grants_p)
    : source_db(std::move(source_db_p)), source_catalog(std::move(source_catalog_p)), grants(std::move(grants_p)) {
}

DuckDBSource::~DuckDBSource() = default;

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
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
		auto entry = Catalog::GetEntry(context, source_catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
		if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
			throw CatalogException("virtual_catalog_bridge: '%s.%s' is no longer on the source", schema, name);
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
		Connection conn(*source_db);
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

DuckDBSession::DuckDBSession(shared_ptr<DatabaseInstance> source_db_p, string source_catalog_p,
                             shared_ptr<GrantBook> grants_p, bool autocommit_p)
    : source_db(std::move(source_db_p)), source_catalog(std::move(source_catalog_p)), grants(std::move(grants_p)),
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
		grant_undo.clear();
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
	return make_uniq<DuckDBSession>(source_db, source_catalog, grants, context.transaction.IsAutoCommit());
}

shared_ptr<Connection> DuckDBSource::PlanningConnection() {
	auto planning = make_shared_ptr<Connection>(*source_db);
	planning->BeginTransaction();
	return planning;
}

CrossingPlan DuckDBSource::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		auto &described = *request.described;
		return CrossingPlan::Of(
		    MakeFloorNode(request.schema, request.table, described.column_names, described.column_types));
	}
	if (request.verb != CrossingVerb::INSERT && request.seam.key_columns.empty()) {
		throw InternalException("virtual_catalog_bridge: '%s.%s' has no key to write by", request.schema,
		                        request.table);
	}
	if (request.seam.key_columns.size() + request.seam.set_columns.size() != request.seam.types.size()) {
		return CrossingPlan::Declined("the seam does not carry one value per column");
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
		CrossingWriteTarget target;
		target.catalog = source_catalog;
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
		BindFloors(context, plan, source_catalog, PolicyResolver(grants, source_catalog));
		return CrossingWriteResult::Done(ExecuteWrite(context, target, std::move(plan), shape));
	};
}

void DuckDBSession::Ddl(ClientContext &, const CrossingDdl &ddl) {
	unique_ptr<SQLStatement> statement;
	idx_t modification;
	switch (ddl.verb) {
	case CrossingVerb::CREATE: {
		auto create = make_uniq<CreateStatement>();
		create->info = ddl.create->Copy();
		create->info->catalog = source_catalog;
		statement = std::move(create);
		modification = DatabaseModificationType::CREATE_CATALOG_ENTRY;
		break;
	}
	case CrossingVerb::ALTER: {
		auto alter = make_uniq<AlterStatement>();
		alter->info = ddl.alter->Copy();
		alter->info->catalog = source_catalog;
		statement = std::move(alter);
		modification = DatabaseModificationType::ALTER_TABLE;
		break;
	}
	case CrossingVerb::DROP: {
		auto drop = make_uniq<DropStatement>();
		drop->info = ddl.drop->Copy();
		drop->info->catalog = source_catalog;
		statement = std::move(drop);
		modification = DatabaseModificationType::DROP_CATALOG_ENTRY;
		break;
	}
	default:
		throw InternalException("virtual_catalog_bridge: '%s' is not a DDL verb", CrossingVerbName(ddl.verb));
	}

	auto ddl_conn = Shared();
	auto &source_conn = *ddl_conn;
	auto creates = ddl.verb != CrossingVerb::CREATE || CreateWouldCreate(source_conn, ddl);
	if (ddl.verb == CrossingVerb::ALTER) {
		ThrowIfAlterOrphansPolicy(ddl);
	}
	auto &attached = Catalog::GetCatalog(*source_conn.context, source_catalog).GetAttached();
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
		RecordDdl(ddl_conn, ddl);
	}
}

bool DuckDBSession::CreateWouldCreate(Connection &source_conn, const CrossingDdl &ddl) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, ddl.table);
	auto existing =
	    Catalog::GetEntry(*source_conn.context, source_catalog, ddl.schema, lookup, OnEntryNotFound::RETURN_NULL);
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

void DuckDBSession::RecordDdl(const shared_ptr<Connection> &source_conn, const CrossingDdl &ddl) {
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
		    Catalog::GetEntry(*source_conn->context, source_catalog, schema_name, lookup, OnEntryNotFound::RETURN_NULL);
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
	auto resolver = PolicyResolver(grants, source_catalog);
	if (autocommit) {
		auto read_conn = make_shared_ptr<Connection>(*source_db);
		read_conn->BeginTransaction();
		return ScanOf(read_conn, RunRead(*read_conn, query, true, resolver, source_catalog));
	}
	auto shared = Shared();
	lock_guard<mutex> guard(lock);
	return ScanOf(shared, RunRead(*shared, query, false, resolver, source_catalog));
}

void RegisterBridgeFunctions(ExtensionLoader &loader) {
	ScalarFunctionSet register_source_set("bridge_register_source");
	ScalarFunction register_source({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                               RegisterSourceFunc);
	register_source.stability = FunctionStability::VOLATILE;
	register_source_set.AddFunction(register_source);
	ScalarFunction register_source_token({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                     LogicalType::VARCHAR, RegisterSourceFunc);
	register_source_token.stability = FunctionStability::VOLATILE;
	register_source_set.AddFunction(register_source_token);
	loader.RegisterFunction(register_source_set);

	ScalarFunctionSet policy_set("bridge_policy");
	ScalarFunction policy_using(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    PolicyFunc);
	policy_using.stability = FunctionStability::VOLATILE;
	policy_set.AddFunction(policy_using);
	ScalarFunction policy_check(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, PolicyFunc);
	policy_check.stability = FunctionStability::VOLATILE;
	policy_set.AddFunction(policy_check);
	loader.RegisterFunction(policy_set);

	ScalarFunction primary_key("bridge_primary_key",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	                           LogicalType::VARCHAR, PrimaryKeyFunc);
	primary_key.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(primary_key);

	ScalarFunction primary_key_query("bridge_primary_key_query",
	                                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, PrimaryKeyQueryFunc);
	primary_key_query.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(primary_key_query);

	ScalarFunction primary_key_check("bridge_primary_key_check",
	                                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::VARCHAR, PrimaryKeyCheckFunc);
	primary_key_check.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(primary_key_check);
}

} // namespace duckdb
