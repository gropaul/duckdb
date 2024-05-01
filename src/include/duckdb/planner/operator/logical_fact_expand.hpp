//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_fact_expand.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {


static ExpressionType MapConditionToFact(const ExpressionType &expression_type) {
	if (expression_type == ExpressionType::COMPARE_EQUAL) {
		return ExpressionType::COMPARE_FACT_EQUAL;
	}

	throw NotImplementedException("Condition not mappable");
}

struct FactChunkBinding {
	// the index of the fact column, counting only the fact columns, so if there are 6 columns in total and 2 fact
	// columns, the max fact column index is 1
	idx_t fact_column_index;
	// the index of the nested column within the fact column
	idx_t nested_column_index;
	// the index of the fact vector within the chunk
	idx_t fact_column_chunk_index;
	//! The alias of the expression,
	string alias;
};


static FactChunkBinding GetFactBindingColumnIndexes(const BoundColumnRefExpression &bound_column, const vector<LogicalType> &types) {
	idx_t fact_column_index = 0;
	for (column_t column_index = 0; column_index < types.size(); column_index++) {
		const LogicalType &type = types[column_index];
		if (type.id() == LogicalTypeId::FACT_POINTER) {
			const FactPointerTypeInfo *type_info = reinterpret_cast<const FactPointerTypeInfo *>(type.AuxInfo());
			for (column_t nested_column_index = 0; nested_column_index < type_info->flat_bindings.size(); nested_column_index++) {
				const ColumnBinding &flat_binding = type_info->flat_bindings[nested_column_index];
				if (flat_binding == bound_column.binding) {
					return {fact_column_index, nested_column_index, column_index, bound_column.alias};
					}
			}

			fact_column_index++;
		}
	}

	throw InternalException("Binding not found");
}

// todo: at one point this should be a join condition with expressions on both sides instead of just columns
struct FactExpandCondition {
public:
	explicit FactExpandCondition(const JoinCondition &condition, const vector<LogicalType> &types) {
		if (condition.right->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_column = reinterpret_cast<BoundColumnRefExpression &>(*condition.right);
			rhs_binding = GetFactBindingColumnIndexes(bound_column, types);
		} else {
			throw NotImplementedException("Only bound column refs are supported for now");
		}
		if (condition.left->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bound_column = reinterpret_cast<BoundColumnRefExpression &>(*condition.left);
			lhs_binding = GetFactBindingColumnIndexes(bound_column, types);

		} else {
			throw NotImplementedException("Only bound column refs are supported for now");
		}
		comparison = MapConditionToFact(condition.comparison);
	}

public:
	// the factorized columns to compare against each other
	FactChunkBinding lhs_binding;
	FactChunkBinding rhs_binding;

	ExpressionType comparison;
};

//! LogicalFactExpand changes factorized DataChunks to a flat DataChunks
class LogicalFactExpand : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_FACT_EXPAND;

public:
	LogicalFactExpand(idx_t table_index, vector<ColumnBinding> flat_bindings, vector<LogicalType> flat_types,
	                  vector<FactExpandCondition> conditions);

private:
	vector<ColumnBinding> flat_bindings;
	vector<LogicalType> flat_types;
	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;
	string ParamsToString() const override;
	// the conditions for the match
	vector<FactExpandCondition> conditions;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
