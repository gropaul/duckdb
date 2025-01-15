
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"
#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalFactorizedPreAggregate &op) {
	D_ASSERT(op.children.size() == 1);
	unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
	auto pre_aggregate = make_uniq<PhysicalFactorizedPreAggregate>(op.types, std::move(op.expressions), op.factor_types, op.estimated_cardinality);
	pre_aggregate->children.push_back(std::move(plan));
	return std::move(pre_aggregate);
}



} // namespace duckdb
