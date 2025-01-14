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
	FactorizedOperatorCollector collector;
	collector.VisitOperator(op);

	const auto matches = collector.GetPotentialMatches();

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
		const auto rhs_bindings = VectorToSet(comparison_join.children[1]->GetColumnBindings());
		return FactorProducer(rhs_bindings, op);
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
				matches.push_back(FactorOperatorMatch(consumer, producer));
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
		FactorizedOperatorCollector collector(consumer_for_children);
		collector.VisitOperator(*child);

		for (auto &match : collector.GetPotentialMatches()) {
			matches.push_back(match);
		}
	}
}

} // namespace duckdb
