#include "duckdb/execution/operator/projection/physical_fact_expand.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFactExpand &op) {
	D_ASSERT(op.children.size() == 1);
	auto child_plan = CreatePlan(*op.children[0]);
	auto fact_expand = make_uniq<PhysicalFactExpand>(std::move(op.types), op.estimated_cardinality, op.conditions);
	fact_expand->children.push_back(std::move(child_plan));

	return std::move(fact_expand);
}

} // namespace duckdb
