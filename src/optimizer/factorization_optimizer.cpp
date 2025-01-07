#include "duckdb/optimizer/factorization_optimizer.hpp"

namespace duckdb {

FactorizationOptimizer::FactorizationOptimizer() {
}

void FactorizationOptimizer::VisitOperator(LogicalOperator &op) {
	auto column_bindings = op.GetColumnBindings();

	if (CanConsumeFactors(op)) {
		const auto consumer = GetFactorConsumer(op);
		consumer.Print();
	}


	if (CanProduceFactors(op)) {
		const auto producer = GetFactorProducer(op);
		producer.Print();
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
		const auto rhs_bindings = comparison_join.children[1]->GetColumnBindings();
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

void GetColumnBindingOfExpression(const unique_ptr<Expression> &expression, vector<ColumnBinding> &collector) {
	switch (expression->GetExpressionClass()) {
	case ExpressionClass::BOUND_CONSTANT:
		break; // nothing to do for this expression
	case ExpressionClass::BOUND_COLUMN_REF: {
		const auto &column_ref = expression->Cast<BoundColumnRefExpression>();
		collector.push_back(column_ref.binding);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		const auto &conjunction = expression->Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.children) {
			GetColumnBindingOfExpression(child, collector);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		const auto &function = expression->Cast<BoundFunctionExpression>();
		for (auto &child : function.children) {
			GetColumnBindingOfExpression(child, collector);
		}
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		const auto &comparison = expression->Cast<BoundComparisonExpression>();
		GetColumnBindingOfExpression(comparison.left, collector);
		GetColumnBindingOfExpression(comparison.right, collector);
		break;
	}
	default:
		const auto class_name = ExpressionClassToString(expression->GetExpressionClass());
		printf("Expression class %s not supported\n", class_name.c_str());
		break;
	}
}

FactorConsumer FactorizationOptimizer::GetFactorConsumer(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		const auto &aggregate = op.Cast<LogicalAggregate>();
		const auto &group_expressions = aggregate.groups;

		vector<ColumnBinding> group_expression_bindings = {};

		for (auto &expression : group_expressions) {
			GetColumnBindingOfExpression(expression, group_expression_bindings);
		}

		return FactorConsumer(group_expression_bindings);
	}
	default: {
		throw NotImplementedException("This operator cannot consume factors");
	}
	}
}

} // namespace duckdb
