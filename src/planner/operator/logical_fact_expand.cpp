#include "duckdb/planner/operator/logical_fact_expand.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalFactExpand::LogicalFactExpand(idx_t table_index, const unique_ptr<LogicalOperator> &child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_FACT_EXPAND), child_column_bindings(child->GetColumnBindings()),
      table_index(table_index) {
	child->ResolveOperatorTypes();
	child_types = child->types;
}

vector<ColumnBinding> LogicalFactExpand::GetColumnBindings() {
	return child_column_bindings;
}

void LogicalFactExpand::ResolveTypes() {
	types = child_types;
}

vector<idx_t> LogicalFactExpand::GetTableIndex() const {
	return vector<idx_t> {table_index};
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
