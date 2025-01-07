//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//! Todo: Add a description
class FactorizationOptimizer : public LogicalOperatorVisitor {
public:
	explicit FactorizationOptimizer();

public:
	void VisitOperator(LogicalOperator &op) override;
};
} // namespace duckdb
