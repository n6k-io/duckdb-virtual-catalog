#include "crossing_write.hpp"

#include "crossing_catalog.hpp"
#include "crossing_scan.hpp"
#include "crossing_transactions.hpp"
#include "internal/seam.hpp"
#include "vcat_key_check.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

struct CrossingWriteSourceState : public GlobalSourceState {
	bool done = false;
};

vector<LogicalType> ColumnTypesByName(CrossingTableCatalogEntry &table, const vector<string> &wanted) {
	vector<LogicalType> out;
	for (auto &want : wanted) {
		out.push_back(table.GetColumns().GetColumn(want).GetType());
	}
	return out;
}

vector<LogicalType> TableRowTypes(CrossingTableCatalogEntry &table) {
	vector<LogicalType> out;
	for (auto &column : table.GetColumns().Logical()) {
		out.push_back(column.GetType());
	}
	return out;
}

vector<string> TableColumnNames(CrossingTableCatalogEntry &table) {
	vector<string> out;
	for (auto &column : table.GetColumns().Logical()) {
		out.push_back(column.GetName());
	}
	return out;
}

vector<string> SetColumnNames(CrossingTableCatalogEntry &table, const vector<PhysicalIndex> &columns) {
	vector<string> out;
	for (auto &column_index : columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName());
	}
	return out;
}

unique_ptr<Expression> RefAt(const LogicalType &type, idx_t position) {
	return make_uniq<BoundReferenceExpression>(type, position);
}

optional_ptr<CrossingTableCatalogEntry> CrossingTableOf(TableCatalogEntry &table) {
	return dynamic_cast<CrossingTableCatalogEntry *>(&table);
}

optional_ptr<LogicalGet> FindGetWithIndex(LogicalOperator &op, idx_t table_index) {
	if (op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().table_index == table_index) {
		return &op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		if (auto found = FindGetWithIndex(*child, table_index)) {
			return found;
		}
	}
	return nullptr;
}

vector<unique_ptr<Expression>> WholeRowFrom(LogicalGet &get, CrossingTableCatalogEntry &table) {
	vector<unique_ptr<Expression>> out;
	for (auto &column : table.GetColumns().Logical()) {
		auto wanted = column.Logical().index;
		auto &ids = get.GetColumnIds();
		idx_t position = ids.size();
		for (idx_t p = 0; p < ids.size(); p++) {
			if (!ids[p].IsVirtualColumn() && ids[p].GetPrimaryIndex() == wanted) {
				position = p;
				break;
			}
		}
		if (position == ids.size()) {
			get.AddColumnId(wanted);
		}
		out.push_back(make_uniq<BoundColumnRefExpression>(column.Name(), column.Type(),
		                                                  ColumnBinding(get.table_index, position)));
	}
	return out;
}

bool MergeDeletes(LogicalMergeInto &merge) {
	for (auto &entry : merge.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_DELETE) {
				return true;
			}
		}
	}
	return false;
}

void WidenForReturning(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = op.Cast<LogicalDelete>();
		auto table = CrossingTableOf(del.table);
		if (!table || !del.return_chunk || del.expressions.empty()) {
			return;
		}
		auto &key = del.expressions[0]->Cast<BoundColumnRefExpression>();
		auto get = FindGetWithIndex(*del.children[0], key.binding.table_index);
		if (!get) {
			return;
		}
		for (auto &ref : WholeRowFrom(*get, *table)) {
			del.expressions.push_back(std::move(ref));
		}
		return;
	}
	if (op.type != LogicalOperatorType::LOGICAL_MERGE_INTO) {
		return;
	}
	auto &merge = op.Cast<LogicalMergeInto>();
	auto table = CrossingTableOf(merge.table);
	if (!table || !merge.return_chunk || !MergeDeletes(merge) ||
	    merge.children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &projection = merge.children[0]->Cast<LogicalProjection>();
	if (merge.row_id_start >= projection.expressions.size()) {
		return;
	}
	auto &key = projection.expressions[merge.row_id_start]->Cast<BoundColumnRefExpression>();
	auto get = FindGetWithIndex(*projection.children[0], key.binding.table_index);
	if (!get) {
		return;
	}
	for (auto &ref : WholeRowFrom(*get, *table)) {
		projection.expressions.push_back(std::move(ref));
	}
}

vector<unique_ptr<Expression>> ImageFromSetValues(CrossingTableCatalogEntry &table, const CrossingSeam &seam,
                                                  const vector<unique_ptr<Expression>> &set_values) {
	vector<unique_ptr<Expression>> image;
	for (auto &name : TableColumnNames(table)) {
		idx_t set = seam.set_columns.size();
		for (idx_t s = 0; s < seam.set_columns.size(); s++) {
			if (StringUtil::CIEquals(seam.set_columns[s], name)) {
				set = s;
				break;
			}
		}
		if (set == seam.set_columns.size()) {
			throw BinderException("virtual_catalog_bridge: RETURNING on '%s' has no value for column '%s'", table.name,
			                      name);
		}
		image.push_back(set_values[set]->Copy());
	}
	return image;
}

struct CrossingWriteGlobalState : public GlobalTableFunctionState {
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<GlobalTableFunctionState> CrossingWriteInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CrossingWriteGlobalState>();
}

void CrossingWriteFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<CrossingWriteGlobalState>();
	if (state.done) {
		return;
	}
	auto &bind_data = data.bind_data->Cast<CrossingWriteBindData>();
	auto affected =
	    RunWrite(context, *bind_data.table.get_mutable(), bind_data.verb, bind_data.seam, *bind_data.fragment, nullptr);
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(affected)));
	state.done = true;
}

InsertionOrderPreservingMap<string> CrossingWriteToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data) {
		return result;
	}
	auto &bind_data = input.bind_data->Cast<CrossingWriteBindData>();
	result["Table"] = bind_data.table->source_schema + "." + bind_data.table->described.Name();
	result["Crossing"] = string(CrossingVerbName(bind_data.verb)) + " on source";
	result["Plan"] = bind_data.fragment->plan_text;
	return result;
}

} // namespace

void RequireVerb(CrossingTableCatalogEntry &table, CrossingVerb verb) {
	if (!table.described.Allows(verb)) {
		throw PermissionException("virtual_catalog_bridge: '%s' does not have '%s' permission", table.name,
		                          CrossingVerbName(verb));
	}
}

void RequireKey(CrossingTableCatalogEntry &table, const char *what) {
	if (table.described.KeyColumns().empty()) {
		throw BinderException("virtual_catalog_bridge: '%s' has no key, so %s cannot address its rows", table.name,
		                      what);
	}
}

void WidenKeyedWritesForReturning(LogicalOperator &plan) {
	for (auto &child : plan.children) {
		WidenKeyedWritesForReturning(*child);
	}
	WidenForReturning(plan);
}

CrossingSeam SeamOf(CrossingTableCatalogEntry &table, CrossingVerb verb, vector<string> set_columns) {
	CrossingSeam seam;
	if (verb == CrossingVerb::INSERT) {
		seam.set_columns = TableColumnNames(table);
		seam.types = TableRowTypes(table);
		return seam;
	}
	seam.key_columns = table.described.KeyColumns();
	seam.set_columns = std::move(set_columns);
	seam.types = ColumnTypesByName(table, seam.key_columns);
	for (auto &type : ColumnTypesByName(table, seam.set_columns)) {
		seam.types.push_back(type);
	}
	return seam;
}

shared_ptr<CrossingFragment> PlanWriteFragment(CrossingTableCatalogEntry &table, CrossingVerb verb,
                                               const CrossingSeam &seam) {
	CrossingPlanRequest request;
	request.verb = verb;
	request.schema = table.source_schema;
	request.table = table.described.Name();
	request.seam = seam;

	auto plan = table.Source().Plan(request);
	if (!plan) {
		throw NotImplementedException("virtual_catalog_bridge: the source has no %s for '%s': %s",
		                              CrossingVerbName(verb), table.name,
		                              request.declined.empty() ? "declined" : request.declined);
	}
	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->seam_types = seam.types;
	fragment->plan = std::move(plan);
	if (!fragment->HasSeam()) {
		throw InternalException("virtual_catalog_bridge: the source's %s of '%s' holds no seam", CrossingVerbName(verb),
		                        table.name);
	}
	fragment->ResolveTypesAndText();
	fragment->VerifyInvariants();
	return fragment;
}

CrossingSeamEntry::CrossingSeamEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     CrossingTableCatalogEntry &target_p, CrossingVerb verb_p, CrossingSeam seam_p,
                                     shared_ptr<CrossingFragment> fragment_p, string obstacle_p)
    : TableCatalogEntry(catalog, schema, info), target(target_p), verb(verb_p), seam(std::move(seam_p)),
      fragment(std::move(fragment_p)), obstacle(std::move(obstacle_p)) {
}

unique_ptr<BaseStatistics> CrossingSeamEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction CrossingSeamEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("virtual_catalog_bridge: a seam is written, never read");
}

TableStorageInfo CrossingSeamEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

TableFunction CrossingWriteFunction() {
	TableFunction function(CROSSING_WRITE_FUNCTION, {}, CrossingWriteFunc, nullptr, CrossingWriteInitGlobal);
	function.to_string = CrossingWriteToString;
	function.projection_pushdown = false;
	return function;
}

string CrossingWritePlanQuery::ToString() const {
	return plan.ToString();
}

vector<CrossingTableUse> CrossingWritePlanQuery::Tables() const {
	CrossingTableUse use;
	use.schema = table.source_schema;
	use.table = table.described.Name();
	auto &names = table.described.ColumnNames();
	auto add = [&](const string &wanted) {
		for (idx_t c = 0; c < names.size(); c++) {
			if (StringUtil::CIEquals(names[c], wanted)) {
				use.columns.push_back(c);
				return;
			}
		}
	};
	for (auto &column : seam.key_columns) {
		add(column);
	}
	for (auto &column : seam.set_columns) {
		add(column);
	}
	return {use};
}

idx_t RunWrite(ClientContext &context, CrossingTableCatalogEntry &table, CrossingVerb verb, const CrossingSeam &seam,
               CrossingFragment &fragment, unique_ptr<ColumnDataCollection> rows) {
	auto slot = fragment.SeamSlot();
	if (slot && (!rows || rows->Count() == 0)) {
		return 0;
	}
	if (!slot && rows) {
		throw InternalException("virtual_catalog_bridge: rows were gathered for a seam a plan already fills");
	}

	unique_ptr<LogicalOperator> seam_node;
	if (slot) {
		seam_node = std::move(*slot);
		auto table_index = seam_node->Cast<LogicalGet>().table_index;
		*slot = make_uniq<LogicalColumnDataGet>(table_index, fragment.seam_types, std::move(rows));
		fragment.ResolveTypesAndText();
	}
	auto restore = [&]() {
		if (seam_node) {
			*slot = std::move(seam_node);
			fragment.ResolveTypesAndText();
		}
	};

	idx_t affected = 0;
	try {
		auto &source = table.Source();
		auto &transaction = TransactionFor(context, source);
		CrossingWritePlanQuery query(verb, table, seam, fragment.seam_types, *fragment.plan);
		affected = source.Write(transaction, query);
	} catch (...) {
		restore();
		throw;
	}
	restore();
	return affected;
}

void CrossingWriteState::SeeKey(DataChunk &chunk, const vector<idx_t> &key_positions, idx_t index) {
	hash_t combined = 0;
	for (idx_t k = 0; k < key_positions.size(); k++) {
		combined = Hash(combined ^ chunk.GetValue(key_positions[k], index).Hash() ^ (0x9E3779B97F4A7C15ULL * (k + 1)));
	}
	seen_keys.insert(combined);
}

CrossingWriteCatalog::CrossingWriteCatalog(AttachedDatabase &db) : DuckCatalog(db) {
}

CrossingWriteCatalog::~CrossingWriteCatalog() = default;

void CrossingWriteCatalog::Initialize(bool load_builtin) {
}

string CrossingWriteCatalog::GetCatalogType() {
	return "virtual_catalog_bridge_write";
}

DatabaseSize CrossingWriteCatalog::GetDatabaseSize(ClientContext &context) {
	return DatabaseSize();
}

bool CrossingWriteCatalog::InMemory() {
	return true;
}

string CrossingWriteCatalog::GetDBPath() {
	return string();
}

static NotImplementedException PassDidNotRun(TableCatalogEntry &table) {
	return NotImplementedException("virtual_catalog_bridge: a write to '%s' needs the crossing optimizer pass",
	                               table.name);
}

PhysicalOperator &CrossingWriteCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalInsert &op, optional_ptr<PhysicalOperator> plan) {
	auto entry = dynamic_cast<CrossingSeamEntry *>(&op.table);
	if (!entry) {
		throw PassDidNotRun(op.table);
	}
	if (!plan) {
		throw InternalException("virtual_catalog_bridge: a seam insert without rows");
	}
	auto &insert = planner.Make<CrossingWrite>(entry->target, entry->verb, entry->seam, entry->fragment, op.types,
	                                           op.estimated_cardinality);
	auto &crossing = insert.Cast<CrossingWrite>();
	crossing.entry = CrossingSeamEntries::Get(context)->Share(*entry);
	crossing.return_chunk = op.return_chunk;
	crossing.obstacle = entry->obstacle;
	crossing.key_positions = entry->key_positions;
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &CrossingWriteCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalDelete &op, PhysicalOperator &plan) {
	throw PassDidNotRun(op.table);
}

PhysicalOperator &CrossingWriteCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalUpdate &op, PhysicalOperator &plan) {
	throw PassDidNotRun(op.table);
}

static unique_ptr<MergeIntoOperator> PlanCrossingMergeAction(ClientContext &context, LogicalMergeInto &op,
                                                             PhysicalPlanGenerator &planner,
                                                             BoundMergeIntoAction &action,
                                                             CrossingTableCatalogEntry &table, idx_t feed_width) {
	auto result = make_uniq<MergeIntoOperator>();
	result->action_type = action.action_type;
	result->condition = std::move(action.condition);

	auto return_types = op.types;
	if (op.return_chunk) {
		return_types.pop_back();
	}
	auto cardinality = op.EstimateCardinality(context);

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE:
	case MergeActionType::MERGE_DELETE: {
		auto verb = action.action_type == MergeActionType::MERGE_UPDATE ? CrossingVerb::UPDATE : CrossingVerb::DELETE_;
		RequireVerb(table, verb);
		auto seam = SeamOf(table, verb, SetColumnNames(table, action.columns));
		auto &keyed =
		    planner
		        .Make<CrossingWrite>(table, verb, seam, PlanWriteFragment(table, verb, seam), return_types, cardinality)
		        .Cast<CrossingWrite>();
		keyed.return_chunk = op.return_chunk;
		auto key_types = ColumnTypesByName(table, seam.key_columns);
		for (idx_t k = 0; k < key_types.size(); k++) {
			keyed.seam_row.push_back(RefAt(key_types[k], op.row_id_start + k));
			keyed.key_positions.push_back(op.row_id_start + k);
		}
		vector<unique_ptr<Expression>> set_values;
		for (idx_t i = 0; i < action.expressions.size(); i++) {
			if (action.expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				set_values.push_back(op.bound_defaults[action.columns[i].index]->Copy());
			} else {
				set_values.push_back(std::move(action.expressions[i]));
			}
		}
		if (op.return_chunk) {
			if (verb == CrossingVerb::DELETE_) {
				auto row_types = TableRowTypes(table);
				for (idx_t c = 0; c < row_types.size(); c++) {
					keyed.returned_row.push_back(RefAt(row_types[c], feed_width - row_types.size() + c));
				}
			} else {
				keyed.returned_row = ImageFromSetValues(table, seam, set_values);
			}
		}
		for (auto &value : set_values) {
			keyed.seam_row.push_back(std::move(value));
		}
		result->op = keyed;
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		RequireVerb(table, CrossingVerb::INSERT);
		auto seam = SeamOf(table, CrossingVerb::INSERT, {});
		auto &insert = planner.Make<CrossingWrite>(table, CrossingVerb::INSERT, seam,
		                                           PlanWriteFragment(table, CrossingVerb::INSERT, seam), return_types,
		                                           cardinality);
		insert.Cast<CrossingWrite>().return_chunk = op.return_chunk;
		result->op = insert;

		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &column : table.GetColumns().Physical()) {
				auto mapped_index = action.column_index_map[column.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					new_expressions.push_back(op.bound_defaults[column.StorageOid()]->Copy());
				} else {
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw NotImplementedException("virtual_catalog_bridge: unsupported MERGE INTO action");
	}
	return result;
}

PhysicalOperator &CrossingWriteCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                      LogicalMergeInto &op, PhysicalOperator &plan) {
	auto &table = op.table.Cast<CrossingTableCatalogEntry>();

	bool addresses_rows = false;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			addresses_rows |= action->action_type == MergeActionType::MERGE_UPDATE ||
			                  action->action_type == MergeActionType::MERGE_DELETE;
		}
	}
	if (addresses_rows) {
		RequireKey(table, "a merge");
	}

	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			planned_actions.push_back(PlanCrossingMergeAction(context, op, planner, *action, table, plan.types.size()));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               false, op.return_chunk);
	result.children.push_back(plan);
	return result;
}

CrossingWrite::CrossingWrite(PhysicalPlan &physical_plan, CrossingTableCatalogEntry &table_p, CrossingVerb verb_p,
                             CrossingSeam seam_p, shared_ptr<CrossingFragment> fragment_p, vector<LogicalType> types_p,
                             idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
      table(table_p), verb(verb_p), seam(std::move(seam_p)), fragment(std::move(fragment_p)) {
}

unique_ptr<GlobalSinkState> CrossingWrite::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CrossingWriteState>();
	state->rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), fragment->seam_types);
	if (return_chunk) {
		state->returned = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), TableRowTypes(table));
	}
	return std::move(state);
}

SinkResultType CrossingWrite::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<CrossingWriteState>();

	for (idx_t i = 0; i < chunk.size() && !key_positions.empty(); i++) {
		state.SeeKey(chunk, key_positions, i);
	}

	if (seam_row.empty()) {
		state.rows->Append(chunk);
	} else {
		DataChunk row;
		row.Initialize(Allocator::DefaultAllocator(), fragment->seam_types);
		ExpressionExecutor executor(context.client, seam_row);
		executor.Execute(chunk, row);
		state.rows->Append(row);
	}

	if (!return_chunk) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (returned_row.empty()) {
		state.returned->Append(chunk);
	} else {
		DataChunk image;
		image.Initialize(Allocator::DefaultAllocator(), TableRowTypes(table));
		ExpressionExecutor executor(context.client, returned_row);
		executor.Execute(chunk, image);
		state.returned->Append(image);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType CrossingWrite::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<CrossingWriteState>();
	if (state.rows->Count() == 0) {
		return SinkFinalizeType::READY;
	}
	auto keys_sent = state.seen_keys.size();
	state.affected_rows = RunWrite(context, table, verb, seam, *fragment, std::move(state.rows));
	if (!key_positions.empty()) {
		EnsureKeyIsUnique(state.affected_rows, keys_sent, table.described.Name(), seam.key_columns,
		                  CrossingVerbName(verb), false);
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> CrossingWrite::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CrossingWriteSourceState>();
}

SourceResultType CrossingWrite::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &source_state = input.global_state.Cast<CrossingWriteSourceState>();
	if (source_state.done) {
		return SourceResultType::FINISHED;
	}
	auto &state = sink_state->Cast<CrossingWriteState>();
	if (return_chunk) {
		if (!state.returned_scanning) {
			state.returned->InitializeScan(state.returned_scan);
			state.returned_scanning = true;
		}
		state.returned->Scan(state.returned_scan, chunk);
		if (chunk.size() == 0) {
			source_state.done = true;
			return SourceResultType::FINISHED;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	source_state.done = true;
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.affected_rows)));
	return SourceResultType::HAVE_MORE_OUTPUT;
}

InsertionOrderPreservingMap<string> CrossingWrite::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = table.source_schema + "." + table.described.Name();
	result["Crossing"] = string(CrossingVerbName(verb)) + " rows";
	if (!obstacle.empty()) {
		result["Not whole because"] = obstacle;
	}
	result["Plan"] = fragment->plan_text;
	return result;
}

} // namespace duckdb
