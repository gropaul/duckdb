

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"


namespace duckdb {

PhysicalFactorizedPreAggregate::PhysicalFactorizedPreAggregate(vector<LogicalType> types, idx_t estimated_cardinality)
	: PhysicalOperator(PhysicalOperatorType::FACTORIZED_PRE_AGGREGATE, std::move(types), estimated_cardinality) {
}

OperatorResultType PhysicalFactorizedPreAggregate::Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
											   GlobalOperatorState &gstate, OperatorState &state_p) const {
	// only reference columns are passed to the pre-aggregate
	output.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb

