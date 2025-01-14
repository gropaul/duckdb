#include "duckdb/optimizer/factorization_optimizer.hpp"
#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

FactorizedPlanRewriter::FactorizedPlanRewriter(LogicalOperator &root, const vector<FactorOperatorMatch> &matches)
    : root(root), matches(matches) {
}

void FactorizedPlanRewriter::Rewrite(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		RewriteComparisonJoin(op.Cast<LogicalComparisonJoin>());
		break;
	};
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		RewriteAggregate(op.Cast<LogicalAggregate>());
		break;
	};
	default:
		break;
	}

	for (auto &child : op.children) {
		Rewrite(*child);
	}
}

void FactorizedPlanRewriter::RewriteAggregate(LogicalAggregate &aggregate) {
	auto pre_aggregate = make_uniq<LogicalFactorizedPreAggregate>();

	// set the child of the pre-aggregate to the child of the aggregate
	pre_aggregate->children.push_back(std::move(aggregate.children[0]));
	// set the child of the aggregate to the pre-aggregate
	aggregate.children[0] = std::move(pre_aggregate);
}

void FactorizedPlanRewriter::RewriteComparisonJoin(LogicalComparisonJoin &join) const {

	const auto flat_bindings = join.GetColumnBindings();

	join.emit_factor_pointers = true;

	join.ResolveOperatorTypes();
	const auto factorized_bindings = join.GetColumnBindings();

	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;

	const idx_t factor_column_index = factorized_bindings.size() - 1;
	const idx_t flat_columns_count = flat_bindings.size();
	const LogicalType factorized_type = LogicalType::POINTER;

	for ( idx_t flat_index = factor_column_index; flat_index < flat_columns_count; flat_index++) {
		const ReplacementBinding replacement(flat_bindings[flat_index], factorized_bindings[factor_column_index], factorized_type);
		replacement_bindings.push_back( replacement );
	}

	replacer.stop_operator = &join;
	replacer.VisitOperator(root);

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

	// all columns that where in the flat binding and are now not in the fact binding anymore are removed

	replacer.stop_operator = end;
	replacer.VisitOperator(*start);

	return make_uniq<ColumnBindingReplacer>(replacer);
}
} // namespace duckdb