//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_fact_expand.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFactExpand changes factorized DataChunks to a flat DataChunks
class LogicalFactExpand : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_FACT_EXPAND;

public:
	LogicalFactExpand(idx_t table_index, vector<ColumnBinding> &flat_bindings, vector<LogicalType> &flat_types);

private:
	vector<ColumnBinding> flat_bindings;
	vector<LogicalType> flat_types;
	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

	bool CanProcessFactVectors() const override {
		return true;
	}

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
