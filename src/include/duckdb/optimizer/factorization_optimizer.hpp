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
	explicit FactorizationOptimizer(Binder &binder);

	//! Optimize Join operators to emit factorized intermediate results
	void Optimize(unique_ptr<LogicalOperator> &op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);

private:
	Binder &binder;
	optional_ptr<LogicalOperator> root;
	void OptimizeInternal(unique_ptr<duckdb::LogicalOperator> &op);

	bool CanProcessFactVectors(unique_ptr<duckdb::LogicalOperator> &op);

};

} // namespace duckdb