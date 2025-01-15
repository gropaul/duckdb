//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalFactorizedPreAggregate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FACTORIZED_PRE_AGGREGATE;

public:
	PhysicalFactorizedPreAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                               vector<LogicalType> factor_types, idx_t estimated_cardinality);

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	vector<unique_ptr<Expression>> aggregates;
	vector<LogicalType> factor_types;

	bool ParallelOperator() const override {
		return true;
	}
};
} // namespace duckdb