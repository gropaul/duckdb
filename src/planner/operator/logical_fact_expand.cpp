#include "duckdb/planner/operator/logical_fact_expand.hpp"

#include "duckdb/main/config.hpp"

#include <utility>

namespace duckdb {

LogicalFactExpand::LogicalFactExpand(idx_t table_index, vector<ColumnBinding> flat_bindings_p,
                                     vector<LogicalType> flat_types_p, vector<FactExpandCondition> conditions_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_FACT_EXPAND), flat_bindings(flat_bindings_p),
      flat_types(flat_types_p), table_index(table_index), conditions(std::move(conditions_p)) {
}

vector<ColumnBinding> LogicalFactExpand::GetColumnBindings() {
	return flat_bindings;
}

void LogicalFactExpand::ResolveTypes() {
	types = flat_types;
}

vector<idx_t> LogicalFactExpand::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalFactExpand::ParamsToString() const {
	string result = "";
	for (auto &condition : conditions) {
		result += "\n";
		auto expr_string = condition.lhs_binding.alias + " " + ExpressionTypeToOperator(condition.comparison) + " " +
		                   condition.rhs_binding.alias;
		result += expr_string;
	}

	return result;
}

string LogicalFactExpand::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb