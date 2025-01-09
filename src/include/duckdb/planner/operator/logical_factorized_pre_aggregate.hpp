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
	LogicalFactorizedPreAggregate();

public:
	vector<ColumnBinding> GetColumnBindings() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
