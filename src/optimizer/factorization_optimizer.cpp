#include "duckdb/optimizer/factorization_optimizer.hpp"

#include "duckdb/core_functions//aggregate/distributive_functions.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
namespace duckdb {

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
	OptimizeInternal(op);

	root->ResolveOperatorTypes();
}

bool FactorizationOptimizer::CanProcessFactVectors(unique_ptr<duckdb::LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_FACT_EXPAND) {
		return true;
	}

	return false;
}

void FactorizationOptimizer::OptimizeInternal(unique_ptr<duckdb::LogicalOperator> &op) {

	vector<unique_ptr<LogicalOperator>> &children = op->children;

	if (!op->CanProcessFactVectors()) {
		for (auto &child : children) {

			if (child->CanEmitFactVectors()) {

				child->ResolveOperatorTypes();
				auto flat_bindings = child->GetColumnBindings();
				auto flat_types = child->types;

				// allow the child to emit fact vectors
				child->SetEmitFactVectors(true);

				child->ResolveOperatorTypes();
				auto fact_bindings = child->GetColumnBindings();
				auto fact_types = child->types;

				// between every operator that can't handle factorization and a factorizable operator, we need to insert
				// a FactorizationExpand
				const auto table_index = binder.GenerateTableIndex();

				auto logical_fact_expand = make_uniq<LogicalFactExpand>(table_index, flat_bindings, flat_types);

				// insert the FactExpand between the two operators
				auto &fact_expand = *logical_fact_expand;
				fact_expand.children.push_back(std::move(child));
				child = std::move(logical_fact_expand);


			}
		}
	}

	// recursively optimize children
	for (auto &child : op->children) {
		OptimizeInternal(child);
	}
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
