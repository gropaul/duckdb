#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"

namespace duckdb {

LogicalFactorizedPreAggregate::LogicalFactorizedPreAggregate()
	: LogicalOperator(LogicalOperatorType::LOGICAL_FACTORIZED_PRE_AGGREGATE) {
}

void LogicalFactorizedPreAggregate::ResolveTypes() {
	types = children[0]->types;
}

vector<ColumnBinding> LogicalFactorizedPreAggregate::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

}