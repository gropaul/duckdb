#include "duckdb/planner/filter/early_probing.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

string EarlyProbingFilter::ToString(const string &column_name) const {
	return column_name + " IN Probe(" + key_column_name + ")";
}

unique_ptr<Expression> EarlyProbingFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant); // todo: I can't really have an expression for this, so this is a hack
}

} // namespace duckdb
