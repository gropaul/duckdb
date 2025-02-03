#include "duckdb/optimizer/factorization_optimizer.hpp"
#include "duckdb/planner/operator/logical_factorized_pre_aggregate.hpp"

namespace duckdb {

static column_binding_set_t VectorToSet(const std::vector<ColumnBinding> &vector) {
	column_binding_set_t set;
	for (const auto &binding : vector) {
		set.insert(binding);
	}
	return set;
}

ColumnBindingCollector::ColumnBindingCollector() {

}

unique_ptr<Expression> ColumnBindingCollector::VisitReplace(BoundColumnRefExpression &expr,
							    unique_ptr<Expression> *expr_ptr) {
	column_references.insert(expr.binding);
	return nullptr;
}

FactorizationOptimizer::FactorizationOptimizer() {
}

void FactorizationOptimizer::VisitOperator(LogicalOperator &op) {

	/*
	// print column bindings and types
	if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE || op.type == LogicalOperatorType::LOGICAL_INSERT) {
		return;
	}

	const auto bindings = op.GetColumnBindings();
	const auto length = bindings.size();
	op.ResolveOperatorTypes();

	printf("Operator: %s\n", op.GetName().c_str());
	for (int i = 0; i < length; i++) {
		const auto &binding = bindings[i];
		const auto &type = op.types[i];
		printf("Binding: %s, Type: %s\n", binding.ToString().c_str(), type.ToString().c_str());
	}

	for (auto &child : op.children) {
		VisitOperator(*child);
	}
	 */
	FactorizedOperatorCollector collector;
	collector.VisitOperator(op);

	const auto &matches = collector.GetPotentialMatches();

	FactorizedPlanRewriter rewriter(op, matches);
	rewriter.Rewrite(op);
}

bool FactorizedOperatorCollector::CanProduceFactors(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		const JoinType join_type = op.Cast<LogicalComparisonJoin>().join_type;
		switch (join_type) {
		case JoinType::MARK:
		case JoinType::SEMI:
		case JoinType::ANTI: {
			return false;
		}
		default: {
			return true;
		}
		}
	}
	default:
		return false;
	}
}

FactorProducer FactorizedOperatorCollector::GetFactorProducer(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		const auto &comparison_join = op.Cast<LogicalComparisonJoin>();

		const auto &build_side_op = comparison_join.children[0];
		const auto rhs_bindings = VectorToSet(build_side_op->GetColumnBindings());

		build_side_op->ResolveOperatorTypes();
		const auto rhs_types = LogicalOperator::MapTypes(build_side_op->types, comparison_join.right_projection_map);


		return FactorProducer(rhs_bindings, rhs_types, op);
	}
	default: {
		throw NotImplementedException("This operator cannot produce factors");
	}
	}
}

bool FactorizedOperatorCollector::CanConsumeFactors(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return true;
	default:
		return false;
	}
}

FactorConsumer FactorizedOperatorCollector::GetFactorConsumer(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		ColumnBindingCollector accumulator;
		for (auto &group_by_expression : op.Cast<LogicalAggregate>().groups) {
			accumulator.VisitExpression(&group_by_expression);
		}

		return FactorConsumer(accumulator.GetColumnReferences(), op);
	}
	default: {
		throw NotImplementedException("This operator cannot consume factors");
	}
	}
}

void FactorizedOperatorCollector::VisitOperator(LogicalOperator &op) {

	if (CanProduceFactors(op)) {
		auto producer = GetFactorProducer(op);

		for (auto &consumer : consumers) {
			if (Match(consumer, producer)) {
				matches.push_back(make_uniq<FactorOperatorMatch>(consumer, producer));
			}
		}
	}

	vector<FactorConsumer> consumer_for_children;
	for (auto &consumers : consumers) {
		if (!HindersConsumption(op, consumers)) {
			consumer_for_children.push_back(consumers);
		}
	}

	if (CanConsumeFactors(op)) {
		const auto consumer = GetFactorConsumer(op);
		consumer_for_children.push_back(consumer);
	}

	for (auto &child : op.children) {
		FactorizedOperatorCollector child_collector(consumer_for_children);
		child_collector.VisitOperator(*child);

		// Access elements through the getter and merge
		auto& child_elements = child_collector.GetPotentialMatches();

		for (auto& element : child_elements) {
			matches.push_back(std::move(element));
		}
	}
}

} // namespace duckdb
