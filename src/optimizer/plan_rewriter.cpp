#include "duckdb/optimizer/factorization_optimizer.hpp"
#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

#include "duckdb/function/function_binder.hpp"

#include <core_functions/aggregate/distributive_functions.hpp>

namespace duckdb {

FactorizedPlanRewriter::FactorizedPlanRewriter(LogicalOperator &root,
                                               const vector<unique_ptr<FactorOperatorMatch>> &matches)
    : root(root), matches(matches) {
}

void FactorizedPlanRewriter::Rewrite(LogicalOperator &op) {

	for (auto &match : matches) {
		auto &consumer_op = match->consumer.op;
		RewriteInternal(consumer_op, match);
	}
}

void FactorizedPlanRewriter::RewriteInternal(LogicalOperator &op, const unique_ptr<FactorOperatorMatch> &match) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		RewriteComparisonJoin(op.Cast<LogicalComparisonJoin>(), match);
		break;
	};
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		RewriteAggregate(op.Cast<LogicalAggregate>(), match);
		break;
	};
	default:
		break;
	}

	for (auto &child : op.children) {
		RewriteInternal(*child, match);
	}
}

class AggregateFunctionReplacer : public LogicalOperatorVisitor {

public:
	explicit AggregateFunctionReplacer() {

	}

protected:
	unique_ptr<Expression> VisitReplace(BoundAggregateExpression &expr, unique_ptr<Expression> *expr_ptr) {
		// rewrite count_star to sum
		if (expr.function.name == "count_star") {

			auto count_fun = SumFun::GetFunctions();

			// // push a new aggregate - COUNT(x)
			// FunctionBinder function_binder(optimizer.context);

			// auto count_aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr, AggregateType::NON_DISTINCT);
			// aggr.expressions.push_back(std::move(count_aggr));
			// constants.push_back(std::move(const_expr));
			// rewrote_map.insert(i);
        }
	}
};

void FactorizedPlanRewriter::RewriteAggregate(LogicalAggregate &aggregate,
                                              const unique_ptr<FactorOperatorMatch> &match) {

	aggregate.ResolveOperatorTypes();

	auto aggregate_bindings = aggregate.GetColumnBindings();
	auto aggregate_types = aggregate.types;

	vector<unique_ptr<Expression>> pre_aggregate_expressions;

	// for aggregate, the column bindings are in the order of groups, expressions, grouping functions
	// to aggregates expressions should now be bound to their output columns which will then come from the pre-aggregate
	const idx_t bindings_offset = aggregate.groups.size();

	for (idx_t i = 0; i < aggregate.expressions.size(); i++) {

		auto &expr = aggregate.expressions[i];
		D_ASSERT(expr->GetExpressionType() == ExpressionType::BOUND_AGGREGATE);

		pre_aggregate_expressions.push_back(expr->Copy());

		auto &aggr_expr = expr->Cast<BoundAggregateExpression>();
		auto &new_aggr_binding = aggregate_bindings[bindings_offset + i];
		auto &new_aggr_type = aggregate_types[bindings_offset + i];
		auto new_aggr_ref = make_uniq<BoundColumnRefExpression>(new_aggr_type, new_aggr_binding);

		// put only the new aggregate reference in the expression
		aggr_expr.children.clear();
		aggr_expr.children.push_back(std::move(new_aggr_ref));
	}

	auto pre_aggregate = make_uniq<LogicalFactorizedPreAggregate>(std::move(pre_aggregate_expressions), aggregate_types,
	                                                              aggregate_bindings, match->producer.factor_types);
	// set the child of the pre-aggregate to the child of the aggregate
	pre_aggregate->children.push_back(std::move(aggregate.children[0]));
	// set the child of the aggregate to the pre-aggregate
	aggregate.children[0] = std::move(pre_aggregate);
}

void FactorizedPlanRewriter::RewriteComparisonJoin(LogicalComparisonJoin &join,
                                                   const unique_ptr<FactorOperatorMatch> &match) const {

	const auto flat_bindings = join.GetColumnBindings();

	join.emit_factor_pointers = true;

	join.ResolveOperatorTypes();
	const auto factorized_bindings = join.GetColumnBindings();

	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;

	const idx_t factor_column_index = factorized_bindings.size() - 1;
	const idx_t flat_columns_count = flat_bindings.size();
	const LogicalType factorized_type = LogicalType::POINTER;

	for (idx_t flat_index = factor_column_index; flat_index < flat_columns_count; flat_index++) {
		const ReplacementBinding replacement(flat_bindings[flat_index], factorized_bindings[factor_column_index],
		                                     factorized_type);
		replacement_bindings.push_back(replacement);
	}

	return;

	replacer.stop_operator = &join;

	const auto &consumer_child = match->consumer.op.children[0];
	replacer.VisitOperator(*consumer_child);
}

} // namespace duckdb