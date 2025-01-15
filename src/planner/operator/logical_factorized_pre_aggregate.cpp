#include <utility>

#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"

namespace duckdb {

LogicalFactorizedPreAggregate::LogicalFactorizedPreAggregate(vector<unique_ptr<Expression>> select_list_p,
                                                             vector<LogicalType> parent_aggregate_types_p,
                                                             vector<ColumnBinding> parent_aggregate_bindings_p,
                                                             vector<LogicalType> factor_types_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_FACTORIZED_PRE_AGGREGATE, std::move(select_list_p)),
      parent_aggregate_bindings(std::move(parent_aggregate_bindings_p)),
      parent_aggregate_types(std::move(parent_aggregate_types_p)), factor_types(std::move(factor_types_p)) {
}

void LogicalFactorizedPreAggregate::ResolveTypes() {
	// resolve the types of the children, the types of the pre-aggregate are the types of the aggregate on top
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}

	types = parent_aggregate_types;
}

vector<ColumnBinding> LogicalFactorizedPreAggregate::GetColumnBindings() {
	return parent_aggregate_bindings;
}

} // namespace duckdb