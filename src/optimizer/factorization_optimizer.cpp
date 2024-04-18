#include "duckdb/optimizer/factorization_optimizer.hpp"

#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

bool FactorizationOptimizer::CanOptimize(duckdb::LogicalOperator &op) {
	return false;
}

unique_ptr<LogicalOperator> FactorizationOptimizer::Optimize(unique_ptr<LogicalOperator> op) {
	return op;
}

} // namespace duckdb