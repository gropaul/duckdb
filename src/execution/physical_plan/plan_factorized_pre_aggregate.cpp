
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"
#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFactorizedPreAggregate &op) {
	D_ASSERT(op.children.size() == 1);
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	auto projection = make_uniq<PhysicalFactorizedPreAggregate>(op.types, op.estimated_cardinality);
	projection->children.push_back(std::move(plan));
	return std::move(projection);
}



} // namespace duckdb
