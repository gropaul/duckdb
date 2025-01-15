//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {
struct FactorConsumer {
	explicit FactorConsumer(column_binding_set_t flat_columns, LogicalOperator &op)
	    : flat_columns(std::move(flat_columns)), op(op) {
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
	LogicalOperator &op;
};

struct FactorProducer {
	FactorProducer(column_binding_set_t factor_columns, vector<LogicalType> factor_types, LogicalOperator &op)
	    : factor_columns(std::move(factor_columns)), factor_types(std::move(factor_types)), op(op) {
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
	vector<LogicalType> factor_types;
	LogicalOperator &op;
};

struct FactorOperatorMatch {
	explicit FactorOperatorMatch(FactorConsumer consumer, FactorProducer producer)
	    : consumer(consumer), producer(producer) {
	}
	FactorConsumer consumer;
	FactorProducer producer;
};

class FactorizedPlanRewriter {
public:
	FactorizedPlanRewriter(LogicalOperator &root, const vector<unique_ptr<FactorOperatorMatch>> &matches);
	void Rewrite(LogicalOperator &op);

private:
	LogicalOperator &root;
	const vector<unique_ptr<FactorOperatorMatch>> &matches;

private:
	void RewriteInternal(LogicalOperator &op, const unique_ptr<FactorOperatorMatch> &match);
	void RewriteAggregate(LogicalAggregate &aggregate, const unique_ptr<FactorOperatorMatch> &match);

	void RewriteComparisonJoin(LogicalComparisonJoin &join, const unique_ptr<FactorOperatorMatch> &match) const;
};



class ColumnBindingCollector final : public LogicalOperatorVisitor {
public:
	explicit ColumnBindingCollector();
	column_binding_set_t GetColumnReferences() {
		return column_references;
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	column_binding_set_t column_references;
};

class FactorizedOperatorCollector final : public LogicalOperatorVisitor {
public:
	FactorizedOperatorCollector() : consumers() {
	}

	explicit FactorizedOperatorCollector(const vector<FactorConsumer> &consumers) : consumers(consumers) {
	}
	vector<unique_ptr<FactorOperatorMatch>> &GetPotentialMatches() {
		return matches;
	}

public:
	void VisitOperator(LogicalOperator &op) override;

protected:
private:
	vector<FactorConsumer> consumers;
	vector<unique_ptr<FactorOperatorMatch>> matches;

private:
	static bool Match(const FactorConsumer &consumer, const FactorProducer &producer) {
		// todo: check if this producer can work with the consumers to create a match
		return true;
	}

	static bool HindersConsumption(LogicalOperator &op, FactorConsumer &consumer) {
		// todo: filter out current sources that are not factorisable because of this operator
		return false;
	}

	static bool CanProduceFactors(LogicalOperator &op);
	static FactorProducer GetFactorProducer(LogicalOperator &op);

	static bool CanConsumeFactors(LogicalOperator &op);
	static FactorConsumer GetFactorConsumer(LogicalOperator &op);
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
};

} // namespace duckdb
