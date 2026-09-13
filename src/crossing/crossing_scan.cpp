#include "crossing_scan.hpp"

#include "crossing_table_entry.hpp"
#include "crossing_transactions.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

namespace {

void CollectFloors(const LogicalOperator &op, vector<CrossingTableUse> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (auto table = get.GetTable()) {
			CrossingTableUse use;
			use.schema = table->schema.name;
			use.table = table->name;
			for (auto &column_index : get.GetColumnIds()) {
				if (column_index.IsVirtualColumn()) {
					continue;
				}
				use.columns.push_back(column_index.GetPrimaryIndex());
			}
			out.push_back(std::move(use));
		}
	}
	for (auto &child : op.children) {
		CollectFloors(*child, out);
	}
}

string TypeList(const vector<LogicalType> &types) {
	vector<string> names;
	for (auto &type : types) {
		names.push_back(type.ToString());
	}
	return StringUtil::Join(names, ", ");
}

void EmitRowIds(Vector &rowid_vec, DataChunk &source_chunk, CrossingScanGlobalState &state) {
	auto row_ids = FlatVector::GetData<row_t>(rowid_vec);
	for (idx_t i = 0; i < source_chunk.size(); i++) {
		row_ids[i] = static_cast<row_t>(state.rows_emitted++);
	}
}

void EmitChunk(CrossingScanGlobalState &state, DataChunk &source_chunk, DataChunk &output) {
	output.SetCardinality(source_chunk.size());
	for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
		if (state.column_ids[idx] == COLUMN_IDENTIFIER_ROW_ID) {
			EmitRowIds(output.data[idx], source_chunk, state);
			continue;
		}
		auto position = state.source_position[idx];
		if (position >= state.source_column_count) {
			output.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(output.data[idx], true);
			continue;
		}
		output.data[idx].Reference(source_chunk.data[position]);
	}
}

unique_ptr<GlobalTableFunctionState> CrossingScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<CrossingScanBindData>();
	if (!bind_data.source) {
		throw InternalException("virtual_catalog_bridge: scan of '%s' has no source", bind_data.source_table);
	}

	if (!bind_data.fragment) {
		throw InternalException("virtual_catalog_bridge: scan of '%s' has no fragment", bind_data.source_table);
	}
	auto &emitted = bind_data.fragment->output_types;

	auto state = make_uniq<CrossingScanGlobalState>();
	state->column_ids = input.column_ids;
	state->source_column_count = emitted.size();

	auto &projected = bind_data.fragment->projected_columns;
	for (auto column : state->column_ids) {
		if (column != COLUMN_IDENTIFIER_ROW_ID && ColumnIndex(column).IsVirtualColumn()) {
			throw InternalException("virtual_catalog_bridge: scan of '%s' was asked for a key alias the pass did not "
			                        "resolve",
			                        bind_data.source_table);
		}
		idx_t position = state->source_column_count;
		for (idx_t p = 0; p < projected.size(); p++) {
			if (projected[p] == column) {
				position = p;
				break;
			}
		}
		state->source_position.push_back(position);
	}
	state->query = make_uniq<CrossingScanQuery>(bind_data.fragment, emitted);
	state->source_chunk.Initialize(Allocator::DefaultAllocator(), emitted);

	auto &source = *bind_data.source.get_mutable();
	auto &transaction = TransactionFor(context, source);
	state->reader = source.Read(transaction, *state->query);
	if (!state->reader) {
		throw InternalException("virtual_catalog_bridge: the source returned no reader for '%s'",
		                        bind_data.source_table);
	}
	return std::move(state);
}

void CrossingScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<CrossingScanGlobalState>();
	if (state.finished) {
		return;
	}
	state.source_chunk.Reset();
	if (!state.reader->Next(state.source_chunk) || state.source_chunk.size() == 0) {
		state.finished = true;
		state.reader.reset();
		return;
	}
	auto &expected = state.query->Types();
	auto produced = state.source_chunk.GetTypes();
	if (produced != expected) {
		throw InvalidInputException("virtual_catalog_bridge: the reader for '%s' filled a chunk of types [%s], the "
		                            "query wants [%s]",
		                            data_p.bind_data->Cast<CrossingScanBindData>().source_table, TypeList(produced),
		                            TypeList(expected));
	}
	EmitChunk(state, state.source_chunk, output);
}

} // namespace

const LogicalOperator &CrossingScanQuery::Plan() const {
	if (!fragment || !fragment->plan) {
		throw InternalException("virtual_catalog_bridge: a read carries no plan");
	}
	return *fragment->plan;
}

string CrossingScanQuery::ToString() const {
	return fragment ? fragment->plan_text : string();
}

vector<CrossingTableUse> CrossingScanQuery::Tables() const {
	vector<CrossingTableUse> out;
	if (fragment && fragment->plan) {
		CollectFloors(*fragment->plan, out);
	}
	return out;
}

BindInfo CrossingScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &data = bind_data->Cast<CrossingScanBindData>();
	if (!data.table) {
		throw InternalException("virtual_catalog_bridge: a scan with no table entry behind it");
	}
	return BindInfo(*data.table.get_mutable());
}

InsertionOrderPreservingMap<string> CrossingScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data) {
		return result;
	}
	auto &bind_data = input.bind_data->Cast<CrossingScanBindData>();
	result["Table"] = bind_data.source_schema + "." + bind_data.source_table;
	if (bind_data.fragment) {
		result["Table Index"] = to_string(bind_data.fragment->table_index);
		result["Plan Columns"] = to_string(bind_data.fragment->output_types.size());
		result["Plan"] = bind_data.fragment->plan_text.empty() ? "(none)" : bind_data.fragment->plan_text;
	}
	return result;
}

TableFunction CrossingScanFunction() {
	TableFunction function(CROSSING_SCAN_FUNCTION, {}, CrossingScanFunc, nullptr, CrossingScanInitGlobal);
	function.to_string = CrossingScanToString;
	function.get_bind_info = CrossingScanGetBindInfo;
	// Without this, column_ids is not the output-slot-to-source-column map the scan reads it as.
	// Filters stay off: crossing wants them as operators to fold, not as a TableFilterSet.
	function.projection_pushdown = true;
	return function;
}

unique_ptr<FunctionData> MakeCrossingScanBindData(CrossingTableCatalogEntry &entry, CrossingSource &source) {
	auto bind_data = make_uniq<CrossingScanBindData>();
	bind_data->source = &source;
	bind_data->source_schema = entry.source_schema;
	bind_data->source_table = entry.described.Name();
	bind_data->column_names = entry.described.ColumnNames();
	bind_data->column_types = entry.described.ColumnTypes();
	bind_data->table = &entry;

	auto fragment = make_shared_ptr<CrossingFragment>();
	fragment->column_names = bind_data->column_names;
	fragment->column_types = bind_data->column_types;
	fragment->table_index = FreshTableIndexBase();

	CrossingPlanRequest request;
	request.verb = CrossingVerb::SELECT;
	request.schema = bind_data->source_schema;
	request.table = bind_data->source_table;
	auto floor = source.Plan(request);
	if (!floor) {
		throw NotImplementedException("virtual_catalog_bridge: the source has no scan of '%s': %s",
		                              bind_data->source_table,
		                              request.declined.empty() ? "declined" : request.declined);
	}
	OffsetTableIndices(*floor, FreshTableIndexBase());
	floor->ResolveOperatorTypes();
	fragment->floor_bindings = floor->GetColumnBindings();
	if (fragment->floor_bindings.size() != fragment->column_names.size()) {
		throw InternalException("virtual_catalog_bridge: the source's scan of '%s' produces %llu columns, its "
		                        "description names %llu",
		                        bind_data->source_table, fragment->floor_bindings.size(),
		                        fragment->column_names.size());
	}
	fragment->SealFloor(*floor);
	fragment->floor = std::move(floor);

	vector<column_t> all_columns;
	for (idx_t i = 0; i < fragment->column_names.size(); i++) {
		all_columns.push_back(i);
	}
	fragment->RebuildPlanForColumns(all_columns);

	bind_data->fragment = std::move(fragment);
	return std::move(bind_data);
}

} // namespace duckdb
