//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

class FactorizationOptimizer {
public:
	//! Optimize Join operators to emit factorized intermediate results
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb