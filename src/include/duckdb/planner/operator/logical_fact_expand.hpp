//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_fact_expand.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// todo: at one point this should be a join condition with expressions on both sides instead of just columns
struct FactExpandCondition {
public:
	FactExpandCondition() {
	}

public:
	// the factorized columns to compare against each other
	column_t lhs_fact_column;
	column_t rhs_fact_column;

	// The flat column inside the fact vector
	column_t lhs_flat_column;
	column_t rhs_flat_column;
	ExpressionType comparison;
};

//! LogicalFactExpand changes factorized DataChunks to a flat DataChunks
class LogicalFactExpand : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_FACT_EXPAND;

public:
	LogicalFactExpand(idx_t table_index, vector<ColumnBinding> flat_bindings, vector<LogicalType> flat_types, vector<FactExpandCondition> conditions);

private:
	vector<ColumnBinding> flat_bindings;
	vector<LogicalType> flat_types;
	idx_t table_index;



public:
	vector<ColumnBinding> GetColumnBindings() override;

	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

	// the conditions for the match
	vector<FactExpandCondition> conditions;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
