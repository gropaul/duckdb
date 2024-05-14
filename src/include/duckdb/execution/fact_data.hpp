#pragma once

#include "memory.h"

namespace duckdb {

const uint64_t NULL_ELEMENT = 0xFFFFFFFFFFFFFFFF;

inline uint64_t FactHash(uint64_t key, uint64_t bitmask) {
	return key & bitmask;
}

inline void IncrementHash(uint64_t &hash, uint64_t bitmask) {
	hash = (hash + 1) & bitmask;
}

inline void FillHtWithIndex(uint64_t *elements, idx_t elements_count, uint64_t *ht, idx_t bitmask) {
	for (idx_t idx = 0; idx < elements_count; idx++) {
		auto element = elements[idx];
		auto hash = FactHash(element, bitmask);
		while (ht[hash] != NULL_ELEMENT) {
			IncrementHash(hash, bitmask);
		}
		ht[hash] = idx;
	}
}

struct fact_data_t { // NOLINT
	void Initialize(idx_t chain_length_p, data_ptr_t chain_head_p, data_ptr_t *pointers_p, uint64_t *keys_p, idx_t ht_capacity_p) {
		chain_length = chain_length_p;
		chain_head = chain_head_p;
		pointers = pointers_p;
		keys = keys_p;
		map_built = false;
		keys_gathered = false;
		ht_capacity = ht_capacity_p;
		// as the capacity is a power of 2, we can use a bitmask to get the hash
		ht_bitmask = ht_capacity - 1;

		// initialize the key map
		// key_map = make_uniq<unordered_map<uint64_t, PointerOccurrence>>();
		// auto _key_map = make_uniq<unordered_map<uint64_t, PointerOccurrence>>();
		// key_map = std::make_unique<unordered_map<uint64_t, PointerOccurrence>>();
	}

	uint64_t chain_length;
	uint64_t ht_capacity;
	uint64_t ht_bitmask;
	data_ptr_t chain_head;

	// the pointers that form the original chain
	data_ptr_t *pointers;
	// the keys for the intersection
	uint64_t *keys;
	// the hashtable that is available
	uint64_t *chain_ht;
	// whether the map has been built
	bool map_built;
	// keys gathered from the chain
	bool keys_gathered;

	bool KeyMapBuilt() {
		return map_built;
	}

	// Builds the key map if it has not been built yet
	void BuildKeyMap() {
		if (map_built) {
			return;
		} else {
			map_built = true;
		}
		FillHtWithIndex(keys, chain_length, chain_ht, ht_bitmask);
	}
};

struct ht_fact_entry_t { // NOLINT
	idx_t chain_length;
	fact_data_t *data;

	explicit ht_fact_entry_t(idx_t chain_length_p) : chain_length(chain_length_p) {
	}

	ht_fact_entry_t IncrementedCopy(ht_fact_entry_t original) {
		return ht_fact_entry_t(original.chain_length += 1);
	}
};

} // namespace duckdb