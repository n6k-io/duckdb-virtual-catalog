#include "duckdb_source.hpp"

#include "crossing_catalog.hpp"

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
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

namespace {

constexpr const char *CROSSING_SEAM_ALIAS = "vcat_seam";

struct PendingV2Source {
	shared_ptr<DatabaseInstance> source_db;
	string token;
	string source_catalog;
	std::chrono::steady_clock::time_point last_touched;
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> schemas;
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
	if (source_entry && dynamic_cast<CrossingCatalog *>(source_entry.get())) {
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
		if (dynamic_cast<CrossingCatalog *>(entry.get())) {
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
                            const vector<string> &row_aliases, idx_t key_count) {
	ParsedExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
		if (child->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &column_ref = child->Cast<ColumnRefExpression>();
			if (!column_ref.IsQualified()) {
				auto &name = column_ref.GetColumnName();
				for (idx_t c = 0; c < set_columns.size(); c++) {
					if (StringUtil::CIEquals(set_columns[c], name)) {
						child = make_uniq<ColumnRefExpression>(row_aliases[key_count + c], CROSSING_SEAM_ALIAS);
						return;
					}
				}
				child = make_uniq<ColumnRefExpression>(name, table);
				return;
			}
		}
		PointCheckAtUpdatedRow(*child, table, set_columns, row_aliases, key_count);
	});
}

unique_ptr<TableRef> SeamShapedRows(const vector<string> &row_aliases, const vector<LogicalType> &types) {
	vector<unique_ptr<ParsedExpression>> row_values;
	for (auto &type : types) {
		row_values.push_back(make_uniq<ConstantExpression>(Value(type)));
	}
	vector<vector<unique_ptr<ParsedExpression>>> values;
	values.push_back(std::move(row_values));
	auto rows_ref = make_uniq<ExpressionListRef>();
	rows_ref->values = std::move(values);
	rows_ref->alias = CROSSING_SEAM_ALIAS;
	rows_ref->expected_names = row_aliases;
	rows_ref->expected_types = types;
	return std::move(rows_ref);
}

unique_ptr<LogicalOperator> *SlotOfExpressionGet(unique_ptr<LogicalOperator> &node) {
	if (!node) {
		return nullptr;
	}
	if (node->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		return &node;
	}
	for (auto &child : node->children) {
		if (auto found = SlotOfExpressionGet(child)) {
			return found;
		}
	}
	return nullptr;
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
		string schema;
		string table;
		SplitGrantName(table_names[i].GetString(), schema, table);

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

			auto source_table = source_conn.Table(pending.source_catalog, schema, table);

			auto using_text = usings[i].GetString();
			auto using_predicate = ParseAndValidatePredicate(source_table, using_text, "USING");
			if (verb == CrossingVerb::INSERT && !IsUnrestricted(using_text)) {
				throw IOException("virtual_catalog_bridge: the USING predicate for '%s' must be literally 'true'",
				                  CrossingVerbName(verb));
			}
			unique_ptr<ParsedExpression> check_predicate;
			if (has_check) {
				auto checks = FlatVector::GetData<string_t>(args.data[4]);
				check_predicate = ParseAndValidatePredicate(source_table, checks[i].GetString(), "WITH CHECK");
			} else if (verb == CrossingVerb::INSERT) {
				throw IOException("virtual_catalog_bridge: an 'insert' grant must state a WITH CHECK predicate");
			} else if (verb == CrossingVerb::UPDATE && using_predicate) {
				check_predicate = using_predicate->Copy();
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

SourceGrant &GrantToKey(PendingV2Source &pending, const string &schema, const string &table, const string &qualified) {
	return pending.schemas[schema][table];
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
		auto &grant = GrantToKey(pending, schema, table, qualified);
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
			if (GrantToKey(pending, schema, table, qualified).key.empty()) {
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
		GrantToKey(pending, schema, table, qualified).key_verified = true;
		result_data[i] = StringVector::AddString(result, "ok");
	}
}

} // namespace

unique_ptr<CrossingSource> RedeemBridgeAttach(ClientContext &, AttachInfo &info) {
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
	case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> schemas;
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
		idx_t table_count = 0;
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
		schemas = std::move(it->second.schemas);
		PendingSources().erase(it);
	}
	return make_uniq<DuckDBSource>(source_db, std::move(source_catalog), std::move(schemas));
}

bool TryParseCrossingVerb(const string &text, CrossingVerb &out) {
	for (idx_t v = 0; v < CROSSING_VERB_COUNT; v++) {
		auto verb = static_cast<CrossingVerb>(v);
		if (StringUtil::CIEquals(text, CrossingVerbName(verb))) {
			out = verb;
			return true;
		}
	}
	return false;
}

DuckDBSource::DuckDBSource(shared_ptr<DatabaseInstance> source_db_p, string source_catalog_p,
                           case_insensitive_map_t<case_insensitive_map_t<SourceGrant>> granted_p)
    : source_db(std::move(source_db_p)), source_catalog(std::move(source_catalog_p)), granted(std::move(granted_p)) {
}

DuckDBSource::~DuckDBSource() = default;

vector<string> DuckDBSource::Schemas() {
	vector<string> out;
	for (auto &schema : granted) {
		out.push_back(schema.first);
	}
	std::sort(out.begin(), out.end());
	return out;
}

vector<string> DuckDBSource::Tables(const string &schema) {
	vector<string> out;
	auto it = granted.find(schema);
	if (it == granted.end()) {
		return out;
	}
	for (auto &table : it->second) {
		out.push_back(table.first);
	}
	std::sort(out.begin(), out.end());
	return out;
}

CrossingTable DuckDBSource::Describe(const string &schema, const string &name) {
	auto schema_it = granted.find(schema);
	if (schema_it == granted.end()) {
		throw CatalogException("virtual_catalog_bridge: nothing is granted in schema '%s'", schema);
	}
	auto table_it = schema_it->second.find(name);
	if (table_it == schema_it->second.end()) {
		throw CatalogException("virtual_catalog_bridge: '%s.%s' is not granted", schema, name);
	}

	Connection conn(*source_db);
	conn.BeginTransaction();
	CrossingTable table(name);
	try {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
		auto entry = Catalog::GetEntry(*conn.context, source_catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
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
			table.Constraint(constraint->Copy());
		}
	} catch (...) {
		conn.Rollback();
		throw;
	}
	conn.Rollback();

	for (idx_t v = 0; v < CROSSING_VERB_COUNT; v++) {
		auto verb = static_cast<CrossingVerb>(v);
		if (table_it->second.Has(verb)) {
			table.Allow(verb);
		}
	}
	if (!table_it->second.key.empty()) {
		if (table_it->second.key_verified) {
			table.UniqueKey(table_it->second.key);
		} else {
			table.Key(table_it->second.key);
		}
	}
	return table;
}

DuckDBTransaction::DuckDBTransaction(shared_ptr<DatabaseInstance> source_db_p, bool autocommit_p)
    : autocommit(autocommit_p), source_db(std::move(source_db_p)) {
}

shared_ptr<Connection> DuckDBTransaction::Shared() {
	lock_guard<mutex> guard(lock);
	if (!conn) {
		conn = make_shared_ptr<Connection>(*source_db);
		conn->BeginTransaction();
	}
	return conn;
}

void DuckDBTransaction::Commit() {
	lock_guard<mutex> guard(lock);
	if (!conn) {
		return;
	}
	conn->Commit();
	conn.reset();
}

void DuckDBTransaction::Rollback() {
	lock_guard<mutex> guard(lock);
	if (!conn) {
		return;
	}
	try {
		conn->Rollback();
	} catch (...) { // NOLINT: the target's transaction is already resolving
	}
	conn.reset();
}

unique_ptr<CrossingTransaction> DuckDBSource::Begin(ClientContext &context) {
	return make_uniq<DuckDBTransaction>(source_db, context.transaction.IsAutoCommit());
}

shared_ptr<Connection> DuckDBSource::PlanningConnection() {
	auto planning = make_shared_ptr<Connection>(*source_db);
	planning->BeginTransaction();
	return planning;
}

optional_ptr<const SourceGrant> DuckDBSource::GrantFor(const string &schema, const string &table) const {
	auto schema_it = granted.find(schema);
	if (schema_it == granted.end()) {
		return nullptr;
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? nullptr : &table_it->second;
}

unique_ptr<SQLStatement> DuckDBSource::InsertStatement(const CrossingPlanRequest &request,
                                                       const vector<string> &row_aliases,
                                                       unique_ptr<TableRef> rows_ref) {
	unique_ptr<ParsedExpression> check;
	if (auto grant = GrantFor(request.schema, request.table)) {
		check = grant->CopyCheck(CrossingVerb::INSERT);
	}

	auto named = make_uniq<SelectNode>();
	named->from_table = std::move(rows_ref);
	for (idx_t c = 0; c < row_aliases.size(); c++) {
		auto column = make_uniq<ColumnRefExpression>(row_aliases[c], CROSSING_SEAM_ALIAS);
		column->alias = request.seam.set_columns[c];
		named->select_list.push_back(std::move(column));
	}
	auto named_select = make_uniq<SelectStatement>();
	named_select->node = std::move(named);

	auto guarded = make_uniq<SelectNode>();
	auto named_ref = make_uniq<SubqueryRef>(std::move(named_select));
	named_ref->alias = "vcat_rows";
	guarded->from_table = std::move(named_ref);
	for (idx_t c = 0; c < request.seam.set_columns.size(); c++) {
		guarded->select_list.push_back(make_uniq<ColumnRefExpression>(request.seam.set_columns[c], "vcat_rows"));
	}
	if (check && !guarded->select_list.empty()) {
		guarded->select_list[0] = GuardWithCheck(std::move(check), std::move(guarded->select_list[0]));
	}
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(guarded);

	auto statement = make_uniq<duckdb::InsertStatement>();
	statement->catalog = source_catalog;
	statement->schema = request.schema;
	statement->table = request.table;
	statement->select_statement = std::move(select);
	return std::move(statement);
}

unique_ptr<SQLStatement> DuckDBSource::DeleteStatement(const CrossingPlanRequest &request,
                                                       const vector<string> &row_aliases,
                                                       unique_ptr<TableRef> rows_ref) {
	auto &key = request.seam.key_columns;
	unique_ptr<ParsedExpression> condition;
	for (idx_t c = 0; c < key.size(); c++) {
		auto target_column = make_uniq<ColumnRefExpression>(key[c], request.table);
		auto seam_column = make_uniq<ColumnRefExpression>(row_aliases[c], CROSSING_SEAM_ALIAS);
		auto equal = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(target_column),
		                                             std::move(seam_column));
		condition = condition ? make_uniq_base<ParsedExpression, ConjunctionExpression>(
		                            ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(equal))
		                      : unique_ptr<ParsedExpression>(std::move(equal));
	}
	if (auto grant = GrantFor(request.schema, request.table)) {
		if (auto using_predicate = grant->CopyUsing(CrossingVerb::DELETE_)) {
			condition = make_uniq_base<ParsedExpression, ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(using_predicate));
		}
	}

	auto statement = make_uniq<duckdb::DeleteStatement>();
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->catalog_name = source_catalog;
	table_ref->schema_name = request.schema;
	table_ref->table_name = request.table;
	statement->table = std::move(table_ref);
	statement->using_clauses.push_back(std::move(rows_ref));
	statement->condition = std::move(condition);
	return std::move(statement);
}

unique_ptr<SQLStatement> DuckDBSource::UpdateStatement(const CrossingPlanRequest &request,
                                                       const vector<string> &row_aliases,
                                                       unique_ptr<TableRef> rows_ref) {
	auto &key = request.seam.key_columns;
	auto &set_columns = request.seam.set_columns;
	unique_ptr<ParsedExpression> condition;
	for (idx_t c = 0; c < key.size(); c++) {
		auto target_column = make_uniq<ColumnRefExpression>(key[c], request.table);
		auto seam_column = make_uniq<ColumnRefExpression>(row_aliases[c], CROSSING_SEAM_ALIAS);
		auto equal = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(target_column),
		                                             std::move(seam_column));
		condition = condition ? make_uniq_base<ParsedExpression, ConjunctionExpression>(
		                            ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(equal))
		                      : unique_ptr<ParsedExpression>(std::move(equal));
	}

	unique_ptr<ParsedExpression> check;
	if (auto grant = GrantFor(request.schema, request.table)) {
		if (auto using_predicate = grant->CopyUsing(CrossingVerb::UPDATE)) {
			condition = make_uniq_base<ParsedExpression, ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(condition), std::move(using_predicate));
		}
		check = grant->CopyCheck(CrossingVerb::UPDATE);
	}
	if (check) {
		PointCheckAtUpdatedRow(*check, request.table, set_columns, row_aliases, key.size());
	}

	auto set_info = make_uniq<UpdateSetInfo>();
	set_info->condition = std::move(condition);
	for (idx_t c = 0; c < set_columns.size(); c++) {
		set_info->columns.push_back(set_columns[c]);
		set_info->expressions.push_back(
		    make_uniq<ColumnRefExpression>(row_aliases[key.size() + c], CROSSING_SEAM_ALIAS));
	}
	if (check && !set_info->expressions.empty()) {
		set_info->expressions[0] = GuardWithCheck(std::move(check), std::move(set_info->expressions[0]));
	}

	auto statement = make_uniq<duckdb::UpdateStatement>();
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->catalog_name = source_catalog;
	table_ref->schema_name = request.schema;
	table_ref->table_name = request.table;
	statement->table = std::move(table_ref);
	statement->from_table = std::move(rows_ref);
	statement->set_info = std::move(set_info);
	return std::move(statement);
}

unique_ptr<LogicalOperator> DuckDBSource::ScanPlan(const CrossingPlanRequest &request) {
	auto planning_conn = PlanningConnection();
	auto &source_conn = *planning_conn;
	auto relation = source_conn.Table(source_catalog, request.schema, request.table);
	if (auto grant = GrantFor(request.schema, request.table)) {
		if (auto using_predicate = grant->CopyUsing(CrossingVerb::SELECT)) {
			relation = relation->Filter(std::move(using_predicate));
		}
	}
	Planner planner(*source_conn.context);
	planner.CreatePlan(make_uniq<RelationStatement>(relation));
	auto plan = std::move(planner.plan);
	plan->ResolveOperatorTypes();
	return plan;
}

unique_ptr<LogicalOperator> DuckDBSource::Plan(CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		return ScanPlan(request);
	}
	if (request.verb != CrossingVerb::INSERT && request.seam.key_columns.empty()) {
		throw InternalException("virtual_catalog_bridge: '%s.%s' has no key to write by", request.schema,
		                        request.table);
	}
	vector<string> row_aliases;
	for (idx_t c = 0; c < request.seam.key_columns.size(); c++) {
		row_aliases.push_back("k" + to_string(c));
	}
	for (idx_t c = 0; c < request.seam.set_columns.size(); c++) {
		row_aliases.push_back("v" + to_string(c));
	}
	if (row_aliases.size() != request.seam.types.size()) {
		request.declined = "the seam does not carry one value per column";
		return nullptr;
	}
	auto rows_ref = SeamShapedRows(row_aliases, request.seam.types);

	unique_ptr<SQLStatement> statement;
	switch (request.verb) {
	case CrossingVerb::INSERT:
		statement = InsertStatement(request, row_aliases, std::move(rows_ref));
		break;
	case CrossingVerb::DELETE_:
		statement = DeleteStatement(request, row_aliases, std::move(rows_ref));
		break;
	default:
		statement = UpdateStatement(request, row_aliases, std::move(rows_ref));
		break;
	}

	auto planning_conn = PlanningConnection();
	Planner planner(*planning_conn->context);
	planner.CreatePlan(std::move(statement));
	auto plan = std::move(planner.plan);

	auto slot = SlotOfExpressionGet(plan);
	if (!slot) {
		throw InternalException("virtual_catalog_bridge: the planned %s holds no seam", CrossingVerbName(request.verb));
	}
	auto table_index = (*slot)->Cast<LogicalExpressionGet>().table_index;
	*slot = MakeSeamNode(table_index, request.seam.types);
	plan->ResolveOperatorTypes();
	return plan;
}

CrossingVerdict DuckDBSource::AcceptsCall(const Expression &expr) {
	auto planning_conn = PlanningConnection();
	auto &source_conn = *planning_conn;
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
	EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function_name);
	auto entry =
	    Catalog::GetEntry(*source_conn.context, INVALID_CATALOG, DEFAULT_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
	if (entry) {
		return CrossingVerdict::Yes();
	}
	EntryLookupInfo aggregate_lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, function_name);
	if (Catalog::GetEntry(*source_conn.context, INVALID_CATALOG, DEFAULT_SCHEMA, aggregate_lookup,
	                      OnEntryNotFound::RETURN_NULL)) {
		return CrossingVerdict::Yes();
	}
	return CrossingVerdict::No("the source has no function " + function_name);
}

CrossingVerdict DuckDBSource::AcceptsType(const LogicalType &type) {
	return CrossingVerdict::Yes();
}

idx_t DuckDBSource::Write(CrossingTransaction &transaction, const CrossingWriteQuery &query) {
	auto write_conn = static_cast<DuckDBTransaction &>(transaction).Shared();
	auto &source_conn = *write_conn;
	auto plan = query.Plan().Copy(*source_conn.context);
	plan->ResolveOperatorTypes();

	auto &attached = Catalog::GetCatalog(*source_conn.context, source_catalog).GetAttached();
	MetaTransaction::Get(*source_conn.context).ModifyDatabase(attached, DatabaseModificationType::UPDATE_DATA);

	auto statement = make_uniq<LogicalPlanStatement>(std::move(plan));
	auto pending = source_conn.context->PendingQuery(std::move(statement), QueryParameters(false));
	if (pending->HasError()) {
		pending->GetErrorObject().Throw("virtual_catalog_bridge: write on source failed: ");
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_bridge: write on source failed: ");
	}
	auto count_chunk = result->Fetch();
	if (count_chunk && count_chunk->size() > 0) {
		return NumericCast<idx_t>(count_chunk->GetValue(0, 0).GetValue<int64_t>());
	}
	return 0;
}

namespace {

unique_ptr<QueryResult> RunRead(Connection &source_conn, const CrossingReadQuery &query, bool stream) {
	auto plan = query.Plan().Copy(*source_conn.context);
	plan->ResolveOperatorTypes();

	auto statement = make_uniq<LogicalPlanStatement>(std::move(plan));
	auto pending = source_conn.context->PendingQuery(std::move(statement), QueryParameters(stream));
	if (pending->HasError()) {
		pending->GetErrorObject().Throw("virtual_catalog_bridge: plan on source failed: ");
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->GetErrorObject().Throw("virtual_catalog_bridge: plan on source failed: ");
	}
	return result;
}

class DuckDBReader : public CrossingReader {
public:
	DuckDBReader(shared_ptr<Connection> conn_p, unique_ptr<QueryResult> result_p)
	    : conn(std::move(conn_p)), result(std::move(result_p)) {
	}

	bool Next(DataChunk &chunk) override {
		auto raw = result->FetchRaw();
		if (!raw || raw->size() == 0) {
			return false;
		}
		chunk.Reference(*raw);
		return true;
	}

private:
	shared_ptr<Connection> conn;
	unique_ptr<QueryResult> result;
};

} // namespace

unique_ptr<CrossingReader> DuckDBSource::Read(CrossingTransaction &transaction, const CrossingReadQuery &query) {
	auto &txn = static_cast<DuckDBTransaction &>(transaction);
	if (txn.autocommit) {
		auto read_conn = PlanningConnection();
		auto result = RunRead(*read_conn, query, true);
		return make_uniq<DuckDBReader>(std::move(read_conn), std::move(result));
	}
	auto shared = txn.Shared();
	lock_guard<mutex> guard(txn.lock);
	return make_uniq<DuckDBReader>(shared, RunRead(*shared, query, false));
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
