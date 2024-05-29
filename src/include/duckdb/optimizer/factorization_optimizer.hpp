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

	struct ProposedOperatorFactEmitInfo {
		explicit ProposedOperatorFactEmitInfo(bool is_able_p, bool is_producing_p, bool is_emitting_p)
		    : is_able(is_able_p), is_producing_fact_vectors(is_producing_p), is_emitting_fact_vectors(is_emitting_p) {
		}

		bool is_able;
		//! Whether the operator will internally produce fact vectors
		bool is_producing_fact_vectors;
		//! Whether the operator will emit fact vectors, can be the case that the op will produce but internally flatten
		//! again
		bool is_emitting_fact_vectors;
	};

	struct OperatorFactEmitInfo : ProposedOperatorFactEmitInfo {

		OperatorFactEmitInfo(ProposedOperatorFactEmitInfo proposal, vector<LogicalType> flat_types_p,
		                     vector<LogicalType> fact_types_p, vector<ColumnBinding> flat_bindings_p,
		                     vector<ColumnBinding> fact_bindings_p, unique_ptr<ColumnBindingReplacer> replacer_p)
		    : ProposedOperatorFactEmitInfo(proposal), types_before(std::move(flat_types_p)),
		      types_after(std::move(fact_types_p)), bindings_before(std::move(flat_bindings_p)),
		      bindings_after(std::move(fact_bindings_p)), replacer(std::move(replacer_p)) {
		}

		static OperatorFactEmitInfo FlatEmission() {
			return OperatorFactEmitInfo(ProposedOperatorFactEmitInfo(false, false, false), {}, {}, {}, {}, nullptr);
		}

		vector<LogicalType> types_before;
		vector<LogicalType> types_after;

		vector<ColumnBinding> bindings_before;
		vector<ColumnBinding> bindings_after;

		unique_ptr<ColumnBindingReplacer> replacer;
	};

	struct OperatorFactProcessingInfo {

		explicit OperatorFactProcessingInfo(vector<bool> flat_child_p) : flat_child(std::move(flat_child_p)) {
		}

		vector<bool> flat_child;
	};

private:
	bool move_fact_conditions_to_expand = false;

	idx_t current_emitter_count;

	Binder &binder;
	optional_ptr<LogicalOperator> root;
	unique_ptr<OperatorFactEmitInfo> OptimizeInternal(unique_ptr<LogicalOperator> &op,
	                                                  optional_ptr<LogicalOperator> parent, idx_t child_idx);

	OperatorFactProcessingInfo CanProcessFactVectors(const duckdb::LogicalOperator *op,
	                                                 const vector<unique_ptr<OperatorFactEmitInfo>> &children_info);

	vector<FactExpandCondition> UpdateForProcess(unique_ptr<LogicalOperator> &op,
	                                             const vector<LogicalType> &fact_types);

	ProposedOperatorFactEmitInfo GetProposedEmission(const unique_ptr<duckdb::LogicalOperator> &op,
	                                                 const vector<unique_ptr<OperatorFactEmitInfo>> &children_info);

	unique_ptr<OperatorFactEmitInfo> ProduceFactVectors(unique_ptr<LogicalOperator> &op,
	                                                 optional_ptr<LogicalOperator> parent, idx_t parent_child_idx,
	                                                 ProposedOperatorFactEmitInfo &proposed_emit_info);
	void AddFactExpandBeforeOperatorAndChild(unique_ptr<duckdb::LogicalOperator> &parent,
	                                         unique_ptr<duckdb::LogicalOperator> &child,
	                                         vector<FactExpandCondition> &conditions);
	bool CanProcessFactVectorsFromChild(const LogicalOperator &op, idx_t child_idx);
};

} // namespace duckdb