#include "internal/table_indices.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <atomic>

namespace duckdb {

namespace {

constexpr idx_t SYNTHETIC_INDEX_BASE = 1ULL << 40;
constexpr idx_t SYNTHETIC_INDEX_STRIDE = 1ULL << 20;

void Shift(idx_t &field, idx_t offset, std::unordered_map<idx_t, idx_t> &moved) {
	auto from = field;
	field += offset;
	moved[from] = field;
}

void ShiftIndicesOf(LogicalOperator &op, idx_t offset, std::unordered_map<idx_t, idx_t> &moved) {
	for (auto &child : op.children) {
		ShiftIndicesOf(*child, offset, moved);
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		Shift(op.Cast<LogicalProjection>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		Shift(aggregate.group_index, offset, moved);
		Shift(aggregate.aggregate_index, offset, moved);
		Shift(aggregate.groupings_index, offset, moved);
		break;
	}
	case LogicalOperatorType::LOGICAL_WINDOW:
		Shift(op.Cast<LogicalWindow>().window_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		Shift(op.Cast<LogicalUnnest>().unnest_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_PIVOT:
		Shift(op.Cast<LogicalPivot>().pivot_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_GET:
		Shift(op.Cast<LogicalGet>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		Shift(op.Cast<LogicalColumnDataGet>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		Shift(op.Cast<LogicalDelimGet>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		Shift(op.Cast<LogicalExpressionGet>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		Shift(op.Cast<LogicalDummyScan>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
		Shift(op.Cast<LogicalCTERef>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		if (join.join_type == JoinType::MARK) {
			Shift(join.mark_index, offset, moved);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		Shift(op.Cast<LogicalSetOperation>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		Shift(op.Cast<LogicalCTE>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		Shift(op.Cast<LogicalInsert>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		Shift(op.Cast<LogicalDelete>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		Shift(op.Cast<LogicalUpdate>().table_index, offset, moved);
		break;
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		Shift(op.Cast<LogicalMergeInto>().table_index, offset, moved);
		break;
	default:
		if (!op.GetTableIndex().empty()) {
			throw InternalException("crossing: cannot move the table index of a %s", LogicalOperatorToString(op.type));
		}
		break;
	}
}

} // namespace

idx_t FreshTableIndexBase() {
	static std::atomic<idx_t> next {1};
	return SYNTHETIC_INDEX_BASE + next.fetch_add(1) * SYNTHETIC_INDEX_STRIDE;
}

void OffsetTableIndices(LogicalOperator &op, idx_t offset) {
	std::unordered_map<idx_t, idx_t> moved;
	ShiftIndicesOf(op, offset, moved);
	TableBindingReplacer replacer(moved, nullptr);
	replacer.VisitOperator(op);
}

} // namespace duckdb
