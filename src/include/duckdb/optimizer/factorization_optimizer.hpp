//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct FactorConsumer {
	explicit FactorConsumer(column_binding_set_t flat_columns) : flat_columns(std::move(flat_columns)) {
	}

	void Print() const {
		Printer::Print("FactorConsumer");
		for (auto &column : flat_columns) {
			Printer::Print(
			    StringUtil::Format("ColumnIndex: %llu, TableIndex: %llu", column.column_index, column.table_index));
		}
	}

	//! The columns that need to be flat, e.g. aggregate keys can be inside the factor
	column_binding_set_t flat_columns;
};

struct FactorProducer {
	explicit FactorProducer(column_binding_set_t factor_columns) : factor_columns(std::move(factor_columns)) {
	}

	void Print() const {
		Printer::Print("FactorProducer");
		for (auto &column : factor_columns) {
			Printer::Print(
			    StringUtil::Format("ColumnIndex: %llu, TableIndex: %llu", column.column_index, column.table_index));
		}
	}

	//! The columns that are factorized
	column_binding_set_t factor_columns;
};

class ColumnBindingAccumulator : public LogicalOperatorVisitor {

public:
	explicit ColumnBindingAccumulator();
	column_binding_set_t GetColumnReferences() {
		return column_references;
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	column_binding_set_t column_references;
};


//! Todo: Add a description
class FactorizationOptimizer : public LogicalOperatorVisitor {
public:
	explicit FactorizationOptimizer();

public:
	void VisitOperator(LogicalOperator &op) override;

private:

private:

	static void AddFactorizedPreAggregate(LogicalAggregate &aggregate);

	static bool CanProduceFactors(LogicalOperator &op);
	FactorProducer GetFactorProducer(LogicalOperator &op);

	static bool CanConsumeFactors(LogicalOperator &op);
	FactorConsumer GetFactorConsumer(LogicalOperator &op);
};

} // namespace duckdb
