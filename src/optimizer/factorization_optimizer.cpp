#include "duckdb/optimizer/factorization_optimizer.hpp"

namespace duckdb {

static column_binding_set_t VectorToSet(const std::vector<ColumnBinding> &vector) {
	column_binding_set_t set;
	for (const auto &binding : vector) {
		set.insert(binding);
	}
	return set;
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

	for (auto &child : op.children) {
		VisitOperator(*child);
	}
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
		return FactorProducer(rhs_bindings);
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

		return FactorConsumer(accumulator.GetColumnReferences());
	}
	default: {
		throw NotImplementedException("This operator cannot consume factors");
	}
	}
}

} // namespace duckdb
