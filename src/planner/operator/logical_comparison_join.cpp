#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/enum_util.hpp"
namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = EnumUtil::ToChars(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr =
		    make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}


bool LogicalComparisonJoin::WillEmitFacts(bool produce_fact_vectors) {

	bool has_factorized_condition = false;
	// return true if we have a fact condition
	for(auto &cond : this->conditions){
		if(cond.comparison == ExpressionType::COMPARE_FACT_EQUAL){
			has_factorized_condition = true;
			break;
		}
	}

	// if we produce fact vectors and there is a fact condition, we will need to flat them to resolve the condition
	// and therefore will not emit fact vectors
	// otherwise we will emit fact vectors
	if (produce_fact_vectors) {
		if (has_factorized_condition) {
			return false;
		} else {
			return true;
		}
	} else {
		return false;
	}
}

} // namespace duckdb
