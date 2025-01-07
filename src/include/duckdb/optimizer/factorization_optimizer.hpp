//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {


struct FactorConsumer {
	explicit FactorConsumer(vector<ColumnBinding> flat_columns) : flat_columns(std::move(flat_columns)) {
	}

	void Print() const {
		Printer::Print("FactorConsumer");
		for (auto &column : flat_columns) {
			Printer::Print(StringUtil::Format("ColumnIndex: %llu, TableIndex: %llu", column.column_index, column.table_index));
		}
	}

	//! The columns that need to be flat, e.g. aggregate keys can be inside the factor
	vector<ColumnBinding> flat_columns;
};

struct FactorProducer {
	explicit FactorProducer(vector<ColumnBinding> factor_columns) : factor_columns(std::move(factor_columns)) {
	}

	void Print() const {
		Printer::Print("FactorProducer");
		for (auto &column : factor_columns) {
			Printer::Print(StringUtil::Format("ColumnIndex: %llu, TableIndex: %llu", column.column_index, column.table_index));
		}
	}

	//! The columns that are factorized
	vector<ColumnBinding> factor_columns;
};

//! Todo: Add a description
class FactorizationOptimizer : public LogicalOperatorVisitor {
public:
	explicit FactorizationOptimizer();

public:
	void VisitOperator(LogicalOperator &op) override;

private:
	static bool CanProduceFactors(LogicalOperator &op);
	static FactorProducer GetFactorProducer(LogicalOperator &op);

	static bool CanConsumeFactors(LogicalOperator &op);
	static FactorConsumer GetFactorConsumer(LogicalOperator &op);
};

} // namespace duckdb
