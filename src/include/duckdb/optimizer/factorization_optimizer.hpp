//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/factorization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"

#include <utility>

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

	struct OperatorFactEmitInfo {

		explicit OperatorFactEmitInfo(bool is_able_p) : is_able(is_able_p), is_emitting_new_fact_vectors(false) {
		}

		OperatorFactEmitInfo(bool is_able_p, bool is_emitting_fact_vectors_p, vector<LogicalType> flat_types_p,
		                     vector<LogicalType> fact_types_p, vector<ColumnBinding> flat_bindings_p,
		                     vector<ColumnBinding> fact_bindings_p, vector<FactExpandCondition> conditions_p)
		    : is_able(is_able_p), is_emitting_new_fact_vectors(is_emitting_fact_vectors_p),
		      types_before(std::move(flat_types_p)), types_after(std::move(fact_types_p)),
		      bindings_before(std::move(flat_bindings_p)), bindings_after(std::move(fact_bindings_p)),
		      conditions(std::move(conditions_p)) {
		}

		bool is_able;
		bool is_emitting_new_fact_vectors;
		vector<LogicalType> types_before;
		vector<LogicalType> types_after;

		vector<ColumnBinding> bindings_before;
		vector<ColumnBinding> bindings_after;

		vector<FactExpandCondition> conditions;
	};

	struct OperatorFactProcessingInfo {

		explicit OperatorFactProcessingInfo(vector<bool> flat_child_p) : flat_child(std::move(flat_child_p)) {
		}

		vector<bool> flat_child;
	};

private:
	idx_t current_emitter_count;

	Binder &binder;
	optional_ptr<LogicalOperator> root;
	OperatorFactEmitInfo OptimizeInternal(unique_ptr<LogicalOperator> &op, optional_ptr<LogicalOperator> parent,
	                                      idx_t child_idx);

	OperatorFactProcessingInfo CanProcessFactVectors(const duckdb::LogicalOperator *op,
	                                                 const vector<OperatorFactEmitInfo> &children_info);
	OperatorFactEmitInfo CanEmitFactVectors(const unique_ptr<duckdb::LogicalOperator> &op,
	                                        const vector<OperatorFactEmitInfo> &children_info);

	OperatorFactEmitInfo EmitFactVectors(unique_ptr<LogicalOperator> &op, optional_ptr<LogicalOperator> parent,
	                                     idx_t parent_child_idx, OperatorFactEmitInfo &proposed_emit_info);
	void AddFactExpandBeforeOperator(unique_ptr<duckdb::LogicalOperator> &child, vector<FactExpandCondition> &conditions);
	bool OperatorCanProcessFactVectors(const LogicalOperator &op, idx_t child_idx, OperatorFactEmitInfo &child_info);
};

} // namespace duckdb