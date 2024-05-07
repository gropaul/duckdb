#include "duckdb/optimizer/factorization_optimizer.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/core_functions//aggregate/distributive_functions.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
namespace duckdb {

using OperatorFactProcessingInfo = FactorizationOptimizer::OperatorFactProcessingInfo;
using OperatorFactEmitInfo = FactorizationOptimizer::OperatorFactEmitInfo;

FactorizationOptimizer::FactorizationOptimizer(Binder &binder_p) : current_emitter_count(0), binder(binder_p) {
}

bool FactorizationOptimizer::CanOptimize(duckdb::LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	return false;
}

void FactorizationOptimizer::Optimize(unique_ptr<LogicalOperator> &op) {
	root = op.get();
	OptimizeInternal(op, nullptr, 0);
	root->ResolveOperatorTypes();
}

bool FactorizationOptimizer::CanProcessFactVectorsFromChild(const LogicalOperator &op, idx_t child_idx,
                                                            unique_ptr<OperatorFactEmitInfo> &child_info) {

	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return true;
		// set this false if we don't want to expand before the expansion
	} else if (op.type == LogicalOperatorType::LOGICAL_FACT_EXPAND) {
		return true;
	}

	return false;
}

vector<FactExpandCondition> FactorizationOptimizer::UpdateForProcess(unique_ptr<LogicalOperator> &op,
                                                                     const vector<LogicalType> &fact_types) {
	// remove all conditions where there is a fact pointer on the lhs to move them up to the fact expand
	vector<FactExpandCondition> expand_conditions;

	// update the comparison operator for fact vectors
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {

		auto join = reinterpret_cast<LogicalComparisonJoin *>(op.get());

		for (auto condition = join->conditions.begin(); condition != join->conditions.end();) {
			auto lhs_return_type = condition->left->return_type;
			auto rhs_return_type = condition->right->return_type;

			if (lhs_return_type.id() == LogicalTypeId::FACT_POINTER) {
				// create a fact expand condition

				// visit the rhs expression to make sure it also operates on the fact vector as we want to move the
				// condition up

				if (move_fact_conditions_to_expand) {

					auto fact_expand_condition = FactExpandCondition(*condition, fact_types);
					expand_conditions.push_back(fact_expand_condition);
					condition = join->conditions.erase(condition);
				} else {
					condition->comparison = ExpressionType::COMPARE_FACT_EQUAL;
					++condition;

					// in this case we will also expand the fact vectors
				}

			} else if (rhs_return_type.id() == LogicalTypeId::FACT_POINTER) {
				throw NotImplementedException("RHS fact pointer not implemented");
			} else {
				++condition;
			}
		}
	}

	return expand_conditions;
}

unique_ptr<OperatorFactEmitInfo>
FactorizationOptimizer::CanEmitFactVectors(const unique_ptr<duckdb::LogicalOperator> &op,
                                           const vector<unique_ptr<OperatorFactEmitInfo>> &children_info) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return make_uniq<OperatorFactEmitInfo>(true);
	}
	return make_uniq<OperatorFactEmitInfo>(false);
}

unique_ptr<ColumnBindingReplacer>
UpdateColumnBindings(optional_ptr<LogicalOperator> start, unique_ptr<LogicalOperator> &end,
                     const vector<ColumnBinding> &flat_bindings, const vector<LogicalType> &flat_types,
                     const vector<ColumnBinding> &fact_bindings, const vector<LogicalType> &fact_types,
                     bool flat_to_fact = true) {
	// update all expressions to behave accordingly
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;

	idx_t new_col_idx = 0;
	idx_t flat_type_index = 0;

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

			idx_t flat_type_count = fact_type_flat_types.size();

			// we are at the end of the factorized vector and can step to the next
			if (flat_type_index == flat_type_count - 1) {
				new_col_idx++;
				flat_type_index = 0;
			} else {
				flat_type_index++;
			}

			// if we want to translate flat types to fact types, we need to replace the old binding with the new binding
			if (flat_to_fact) {
				replacement_bindings.emplace_back(old_binding, new_binding, new_type);
			} else {
				replacement_bindings.emplace_back(new_binding, old_binding, old_type);
			}

		} else {
			new_col_idx++;
		}
	}

	replacer.stop_operator = end;
	replacer.VisitOperator(*start);

	return make_uniq<ColumnBindingReplacer>(replacer);
}

unique_ptr<OperatorFactEmitInfo>
FactorizationOptimizer::EmitFactVectors(unique_ptr<LogicalOperator> &op, optional_ptr<LogicalOperator> parent,
                                        idx_t parent_child_idx, unique_ptr<OperatorFactEmitInfo> &proposed_emit_info) {

	// make sure that the parent is never null as we can't emit fact vectors as the query result
	D_ASSERT(parent != nullptr);

	op->ResolveOperatorTypes();
	auto flat_bindings = op->GetColumnBindings();
	auto flat_types = op->types;

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto join = reinterpret_cast<LogicalComparisonJoin *>(op.get());
		// online
		join->SetEmitFactVectors(true, current_emitter_count);
		current_emitter_count++;

	} else {
		throw NotImplementedException("EmitFactVectors not implemented for this operator");
	}
	op->ResolveOperatorTypes();
	auto fact_bindings = op->GetColumnBindings();
	auto fact_types = op->types;

	// todo: this is farly hacky: At some point we have to make serious thoughts on how far up the factorization
	auto replacer = UpdateColumnBindings(op, op, flat_bindings, flat_types, fact_bindings, fact_types, true);

	return make_uniq<OperatorFactEmitInfo>(true, true, flat_types, fact_types, flat_bindings, fact_bindings,
	                                       std::move(replacer));
}

bool DoesProduceFactors(unique_ptr<LogicalOperator> &op) {
	op->ResolveOperatorTypes();
	for (const auto &type : op->types) {
		if (type.id() == LogicalTypeId::FACT_POINTER) {
			return true;
		}
	}

	return false;
}

unique_ptr<OperatorFactEmitInfo> FactorizationOptimizer::OptimizeInternal(unique_ptr<LogicalOperator> &op,
                                                                          optional_ptr<LogicalOperator> parent,
                                                                          idx_t parent_child_idx) {

	// recursively optimize children
	vector<unique_ptr<OperatorFactEmitInfo>> children_info;
	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
		auto &child = op->children[child_idx];
		auto op_info = OptimizeInternal(child, op.get(), child_idx);
		children_info.push_back(std::move(op_info));
	}

	// check if we can emit fact vectors
	auto proposed_emission = CanEmitFactVectors(op, children_info);

	// for each of the childs, either adapt to the fact vectors or add fact expand between
	for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {

		auto &child = op->children[child_idx];

		bool child_produces_factors = DoesProduceFactors(child);

		if (child_produces_factors) {

			auto &child_info = children_info[child_idx];
			child_info->replacer->stop_operator = child.get();
			child_info->replacer->VisitOperator(*op);

			bool can_process_child_facts = CanProcessFactVectorsFromChild(*op, child_idx, proposed_emission);
			auto expand_conditions = UpdateForProcess(op, child->types);

			if (!can_process_child_facts) {
				AddFactExpandBeforeOperatorAndChild(op, child, expand_conditions);
			}
		}
	}

	// now whether this operator will emit factors to the next operator
	auto parent_can_process =
	    parent != nullptr && CanProcessFactVectorsFromChild(*parent, parent_child_idx, proposed_emission);

	// todo: Overwrite to always emit fact vectors
	// parent_can_process = true;
	if (proposed_emission->is_able && parent_can_process) {
		unique_ptr<OperatorFactEmitInfo> actual_emission =
		    EmitFactVectors(op, parent.get(), parent_child_idx, proposed_emission);
		return actual_emission;
	} else {
		return proposed_emission;
	}
}

std::pair<vector<LogicalType>, vector<ColumnBinding>>
GetFlattenedTypesAndBindings(const vector<LogicalType> &types, const vector<ColumnBinding> &bindings) {

	vector<LogicalType> flat_types;
	vector<ColumnBinding> flat_bindings;

	for (size_t col_idx = 0; col_idx < types.size(); ++col_idx) {
		const auto &type = types[col_idx];
		if (type.id() == LogicalTypeId::FACT_POINTER) {
			// Assuming FACT_POINTER means we need to expand
			const FactPointerTypeInfo *type_info = reinterpret_cast<const FactPointerTypeInfo *>(type.AuxInfo());
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

void FactorizationOptimizer::AddFactExpandBeforeOperatorAndChild(unique_ptr<duckdb::LogicalOperator> &parent,
                                                                 unique_ptr<duckdb::LogicalOperator> &child,
                                                                 vector<FactExpandCondition> &conditions) {

	// between every operator that can't handle factorization and a factorizable operator, we need to insert
	// a FactorizationExpand
	const auto table_index = binder.GenerateTableIndex();

	auto fact_types = child->types;
	auto fact_bindings = child->GetColumnBindings();

	child->ResolveOperatorTypes();
	auto flat_types_and_bindings = GetFlattenedTypesAndBindings(fact_types, fact_bindings);
	auto &flat_types = flat_types_and_bindings.first;
	auto &flat_bindings = flat_types_and_bindings.second;

	auto logical_fact_expand = make_uniq<LogicalFactExpand>(table_index, flat_bindings, flat_types, conditions);

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
