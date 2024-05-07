

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
namespace duckdb {

static TupleDataCollection *FindDataCollectionInOp(const PhysicalOperator *op, const idx_t &emitter_id) {

	if (op->type == PhysicalOperatorType::HASH_JOIN) {
		auto physical_hash_join_op = reinterpret_cast<const PhysicalHashJoin *>(op);
		auto collection = physical_hash_join_op->GetHTDataCollection(emitter_id);
		// can be null e.g. if wrong emitter id
		if (collection != nullptr) {
			return collection;
		}
	}

	for (auto &child : op->children) {
		auto child_data_collection = FindDataCollectionInOp(child.get(), emitter_id);
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
struct PointerOccurrence {
	PointerOccurrence() : pointer(nullptr), occurrencies(0) {
	}
	PointerOccurrence(data_ptr_t pointer) : pointer(pointer), occurrencies(1) {
	}
	data_ptr_t pointer;
	uint64_t occurrencies;
};

struct ChainIntersectionResult {
	vector<data_ptr_t> pointers;
};

struct ChainData {

	// the pointers that form the original chain
	vector<data_ptr_t> pointers;

	// the keys for the intersection
	vector<uint64_t> keys;

	// holds how often a key occurs in the chain
	unordered_map<uint64_t, PointerOccurrence> key_map;

	// whether the map has been built
	bool map_built = false;

	bool KeyMapBuilt() {
		return map_built;
	}

	// Builds the key map if it has not been built yet
	void BuildKeyMap() {

		if (map_built) {
			return;
		}

		map_built = true;
		// reserve space for the key map
		key_map.reserve(keys.size());

		for (idx_t i = 0; i < keys.size(); i++) {
			auto key = keys[i];
			auto pointer = pointers[i];
			auto entry = key_map.find(key);
			if (entry == key_map.end()) {
				auto new_occurrence = PointerOccurrence(pointer);
				key_map[key] = new_occurrence;
			} else {
				entry->second.occurrencies++;
			}
		}
	}

	void Probe(const ChainData &probe_data, vector<data_ptr_t> &lhs_pointers, vector<data_ptr_t> &rhs_pointers) {

		// must be built
		D_ASSERT(KeyMapBuilt());

		for (idx_t i = 0; i < probe_data.keys.size(); i++) {
			auto lhs_key = probe_data.keys[i];
			auto lhs_pointer = probe_data.pointers[i];
			auto entry = key_map.find(lhs_key);
			if (entry != key_map.end()) {
				auto occurence = entry->second;
				auto rhs_pointer = occurence.pointer;
				for (idx_t j = 0; j < occurence.occurrencies; j++) {
					lhs_pointers.push_back(lhs_pointer);
					rhs_pointers.push_back(rhs_pointer);
				}
			}
		}

		// must be equal in length
		D_ASSERT(lhs_pointers.size() == rhs_pointers.size());
	}
};

} // namespace duckdb