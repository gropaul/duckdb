//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_factorized_pre_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Todo
class LogicalFactorizedPreAggregate : public LogicalOperator {
public:
	static constexpr auto TYPE = LogicalOperatorType::LOGICAL_FACTORIZED_PRE_AGGREGATE;

public:
	explicit LogicalFactorizedPreAggregate(
		vector<unique_ptr<Expression>> select_list_p,
		vector<LogicalType> parent_aggregate_types_p,
		vector<ColumnBinding> parent_aggregate_bindings_p,
		vector<LogicalType> factor_types_p
		);

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<LogicalType> factor_types;

private:
	vector<ColumnBinding> parent_aggregate_bindings;
	vector<LogicalType> parent_aggregate_types;


protected:
	void ResolveTypes() override;
};
} // namespace duckdb
