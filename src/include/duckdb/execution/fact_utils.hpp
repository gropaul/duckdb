#pragma once

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/execution/fact_data.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {

static inline JoinHashTable* FindHTFromOp(const PhysicalOperator *op, const idx_t &emitter_id) {

	if (op->type == PhysicalOperatorType::HASH_JOIN) {
		auto physical_hash_join_op = reinterpret_cast<const PhysicalHashJoin *>(op);
		auto hash_table = physical_hash_join_op->GetHashTable(emitter_id);
		// can be null e.g. if wrong emitter id
		if (hash_table != nullptr) {
			return hash_table;
		}
	}

	for (auto &child : op->children) {
		auto child_data_collection = FindHTFromOp(child.get(), emitter_id);
		if (child_data_collection) {
			return child_data_collection;
		}
	}

	return nullptr;
}

static vector<LogicalType> FlattenTypes(const vector<LogicalType> &factored_types) {
	vector<LogicalType> flat_types;
	flat_types.reserve(factored_types.size());

	for (const auto &factored_type : factored_types) {
		if (factored_type.id() == LogicalTypeId::FACT_POINTER) {
			auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(factored_type.AuxInfo());
			vector<LogicalType> fact_type_flat_types = type_info->flat_types;
			flat_types.insert(flat_types.end(), fact_type_flat_types.begin(), fact_type_flat_types.end());
		} else {
			flat_types.push_back(factored_type);
		}
	}

	return flat_types;
};

static vector<ColumnBinding> FlattenBindings(const vector<ColumnBinding> &factored_bindings,
                                             const vector<LogicalType> &factored_types) {
	vector<ColumnBinding> flat_bindings;
	flat_bindings.reserve(factored_bindings.size());

	for (idx_t i = 0; i < factored_bindings.size(); i++) {
		auto &factored_binding = factored_bindings[i];
		auto &factored_type = factored_types[i];
		if (factored_type.id() == LogicalTypeId::FACT_POINTER) {
			auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(factored_type.AuxInfo());
			vector<ColumnBinding> fact_type_flat_bindings = type_info->flat_bindings;
			flat_bindings.insert(flat_bindings.end(), fact_type_flat_bindings.begin(), fact_type_flat_bindings.end());
		} else {
			flat_bindings.push_back(factored_binding);
		}
	}

	return flat_bindings;
};

} // namespace duckdb