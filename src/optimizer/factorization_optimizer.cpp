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

ColumnBindingAccumulator::ColumnBindingAccumulator() {
}

unique_ptr<Expression> ColumnBindingAccumulator::VisitReplace(BoundColumnRefExpression &expr,
							    unique_ptr<Expression> *expr_ptr) {
	column_references.insert(expr.binding);
	return nullptr;
}

FactorizationOptimizer::FactorizationOptimizer() {
}

void FactorizationOptimizer::VisitOperator(LogicalOperator &op) {
	auto column_bindings = op.GetColumnBindings();

	if (CanConsumeFactors(op)) {
		const auto consumer = GetFactorConsumer(op);
	}

	if (CanProduceFactors(op)) {
		const auto producer = GetFactorProducer(op);
	}

	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		AddFactorizedPreAggregate(op.Cast<LogicalAggregate>());
	}

	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

void FactorizationOptimizer::AddFactorizedPreAggregate(LogicalAggregate &aggregate) {
	auto pre_aggregate = make_uniq<LogicalFactorizedPreAggregate>();

	// set the child of the pre-aggregate to the child of the aggregate
	pre_aggregate->children.push_back(std::move(aggregate.children[0]));
	// set the child of the aggregate to the pre-aggregate
	aggregate.children[0] = std::move(pre_aggregate);
}

bool FactorizationOptimizer::CanProduceFactors(LogicalOperator &op) {
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

FactorProducer FactorizationOptimizer::GetFactorProducer(LogicalOperator &op) {
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

bool FactorizationOptimizer::CanConsumeFactors(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return true;
	default:
		return false;
	}
}

FactorConsumer FactorizationOptimizer::GetFactorConsumer(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		ColumnBindingAccumulator accumulator;
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

} // namespace duckdb
