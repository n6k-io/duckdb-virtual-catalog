#include "crossing_pass.hpp"

#include "crossing_shape.hpp"
#include "crossing_write.hpp"
#include "internal/pass.hpp"

namespace duckdb {

void CrossingMoveWorkPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	ResolveKeyAliases(*plan);
	WidenKeyedWritesForReturning(*plan);
	MoveCrossableWorkIntoFragments(plan);
	ShapeWrites(input.context, plan);
}

void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	NarrowFragmentsToTheirScans(plan);
}

} // namespace duckdb
