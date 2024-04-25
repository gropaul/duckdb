#include "duckdb/optimizer/factorization_optimizer.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/core_functions//aggregate/distributive_functions.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
namespace duckdb {
using OPFactProcessingInfo = FactorizationOptimizer::OperatorFactProcessingInfo;
using OPFactEmitInfo = FactorizationOptimizer::OperatorFactEmitInfo;
FactorizationOptimizer::FactorizationOptimizer(Binder &binder_p) : binder(binder_p) {
}
bool FactorizationOptimizer::CanOptimize(duckdb::LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	return false;
}

void FactorizationOptimizer::Optimize(unique_ptr<LogicalOperator> &op) {
	root = op.get();
	OptimizeInternal(op, nullptr);
	root->ResolveOperatorTypes();
}

OPFactProcessingInfo FactorizationOptimizer::CanProcessFactVectors(const unique_ptr<duckdb::LogicalOperator> &op,
                                                                   const vector<OperatorFactEmitInfo> &children_info) {
	idx_t child_count = children_info.size();
	vector<bool> all_true(child_count, true);
	vector<bool> all_false(child_count, false);
	if (op->type == LogicalOperatorType::LOGICAL_FACT_EXPAND) {
		return OPFactProcessingInfo(all_false);
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return OPFactProcessingInfo(all_false);
	}
	return OPFactProcessingInfo(all_true);
}

OPFactEmitInfo FactorizationOptimizer::CanEmitFactVectors(const unique_ptr<duckdb::LogicalOperator> &op,
                                                          const vector<OperatorFactEmitInfo> &children_info) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return OPFactEmitInfo(true);
	}
	return OPFactEmitInfo(false);
}

ExpressionType MapCondition(const ExpressionType &expression_type) {
	if (expression_type == ExpressionType::COMPARE_EQUAL) {
		return ExpressionType::COMPARE_FACT_EQUAL;
	}

	throw NotImplementedException("Condition not mappable");
}

OPFactEmitInfo FactorizationOptimizer::EmitFactVectors(unique_ptr<LogicalOperator> &op, optional_ptr<LogicalOperator> parent){

	// make sure that the parent is never null as we can't emit fact vectors as the query result
	D_ASSERT(parent != nullptr);

	op->ResolveOperatorTypes();
	auto flat_bindings = op->GetColumnBindings();
	auto flat_types = op->types;

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto join = reinterpret_cast<LogicalComparisonJoin *>(op.get());
		join->SetEmitFactVectors(true);

	} else {
		throw NotImplementedException("EmitFactVectors not implemented for this operator");
	}
	op->ResolveOperatorTypes();
	auto fact_bindings = op->GetColumnBindings();
	auto fact_types = op->types;

	// update all expressions to behave accordingly
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;

	idx_t new_col_idx = 0;

	for (idx_t old_col_idx = 0; old_col_idx < flat_bindings.size(); old_col_idx++) {

		const auto &old_binding = flat_bindings[old_col_idx];
		const auto &old_type = flat_types[old_col_idx];

		const auto &new_type = fact_types[new_col_idx];
		const auto &new_binding = fact_bindings[new_col_idx];

		// if the type is factorized and the previous was not factorized, we need to point all columns to the factor
		// column.
		// If we access the last factorized element, we step the col index
		if (new_type.id() == LogicalTypeId::FACT_POINTER && old_type.id() != LogicalTypeId::FACT_POINTER) {
			auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(new_type.AuxInfo());
			vector<LogicalType> fact_type_flat_types = type_info->flat_types;

			idx_t flat_type_index = old_col_idx - new_col_idx;
			idx_t flat_type_count = fact_type_flat_types.size();

			// we are at the end of the factorized vector and can step to the next
			if (flat_type_index == flat_type_count - 1) {
				new_col_idx++;
			}
			replacement_bindings.emplace_back(old_binding, new_binding, new_type);

		} else {
			new_col_idx++;
		}
	}

	replacer.stop_operator = op;
	replacer.VisitOperator(*op);

	// update the comparison operator for fact vectors
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto join = reinterpret_cast<LogicalComparisonJoin *>(op.get());
		for (auto &condition : join->conditions) {
			if (condition.left->return_type.id() == LogicalTypeId::FACT_POINTER ||
			    condition.right->return_type.id() == LogicalTypeId::FACT_POINTER) {
				condition.comparison = MapCondition(condition.comparison);
			}
		}
	}

	return {true, true, flat_types, fact_types, flat_bindings, fact_bindings};
}

OPFactEmitInfo FactorizationOptimizer::OptimizeInternal(unique_ptr<LogicalOperator> &op, optional_ptr<LogicalOperator> parent) {

	// initialize a list the size of the children
	vector<OPFactEmitInfo> children_info;

	// recursively optimize children
	for (auto &child : op->children) {
		auto op_info = OptimizeInternal(child, op.get());
		children_info.push_back(op_info);
	}

	// check if we can process fact vectors
	auto process_info = CanProcessFactVectors(op, children_info);

	// if not able to process fact vectors, we need to add a FactExpand
	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
		auto &child = op->children[child_idx];
		auto &child_info = children_info[child_idx];

		// flatten the children if they are emitting fact vectors, and we can't process fact vectors
		if (process_info.flat_child[child_idx] && child_info.is_emitting_fact_vectors) {
			AddFactExpandBeforeOperator(child, child_info);
		}
	}

	// check if we can emit fact vectors
	auto emit_info = CanEmitFactVectors(op, children_info);
	if (emit_info.is_able) {
		emit_info = EmitFactVectors(op, parent.get());
	}

	return emit_info;
}


std::pair<vector<LogicalType>, vector<ColumnBinding>> GetFlattenedTypesAndBindings(
    const vector<LogicalType> &types,
    const vector<ColumnBinding> &bindings) {

	vector<LogicalType> flat_types;
	vector<ColumnBinding> flat_bindings;

	for (size_t col_idx = 0; col_idx < types.size(); ++col_idx) {
		const auto &type = types[col_idx];
		if (type.id() == LogicalTypeId::FACT_POINTER) {
			// Assuming FACT_POINTER means we need to expand
			const FactPointerTypeInfo* type_info = reinterpret_cast<const FactPointerTypeInfo*>(type.AuxInfo());
			flat_types.insert(flat_types.end(), type_info->flat_types.begin(), type_info->flat_types.end());
			flat_bindings.insert(flat_bindings.end(), type_info->flat_bindings.begin(), type_info->flat_bindings.end());
		} else {
			// If not a FACT_POINTER, add directly
			flat_types.push_back(type);
			flat_bindings.push_back(bindings[col_idx]);
		}
	}

	return std::make_pair(flat_types, flat_bindings);
}

void FactorizationOptimizer::AddFactExpandBeforeOperator(unique_ptr<duckdb::LogicalOperator> &child,
                                                      OPFactEmitInfo &child_info) {

	// between every operator that can't handle factorization and a factorizable operator, we need to insert
	// a FactorizationExpand
	const auto table_index = binder.GenerateTableIndex();

	auto flat_types_and_bindings = GetFlattenedTypesAndBindings(child_info.types_before, child_info.bindings_before);
	auto &flat_types = flat_types_and_bindings.first;
	auto &flat_bindings = flat_types_and_bindings.second;

	auto logical_fact_expand = make_uniq<LogicalFactExpand>(table_index, flat_bindings, flat_types);

	// insert the FactExpand between the two operators
	auto &fact_expand = *logical_fact_expand;
	fact_expand.children.push_back(std::move(child));
	child = std::move(logical_fact_expand);
}

void ChangeAggrSumToCount(unique_ptr<duckdb::LogicalOperator> &op) {

	const auto bindings = op->GetColumnBindings();

	auto &aggregate = op->Cast<LogicalAggregate>();

	auto &expressions = aggregate.expressions;
	for (auto &expr : expressions) {
		if (expr->type == ExpressionType::BOUND_AGGREGATE) {
			auto &agg = expr->Cast<BoundAggregateExpression>();
			auto &base_aggr_ref_children = agg.children;

			vector<unique_ptr<Expression>> count_aggr_children;
			for (auto &child : base_aggr_ref_children) {
				auto copy = child->Copy();
				count_aggr_children.push_back(std::move(copy));
			}
			auto count_aggr = make_uniq<BoundAggregateExpression>(
			    CountFun::GetFunction(), std::move(count_aggr_children), nullptr, nullptr, AggregateType::NON_DISTINCT);
			// replace the aggregate with count(*)
			expr = std::move(count_aggr);
		}
	}
	op->ResolveOperatorTypes();

	auto new_bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	auto &new_types = op->types;

	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &old_binding = bindings[col_idx];
		const auto &new_binding = new_bindings[col_idx];
		const auto &new_type = new_types[col_idx];
		replacement_bindings.emplace_back(old_binding, new_binding, new_type);
	}

	// Make the plan consistent again
	// replacer.VisitOperator(*root);

	// todo: stop at aggregation operator
}

} // namespace duckdb
