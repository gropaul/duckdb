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

// Define the enum for the states
enum class FactDataState : uint8_t {
	KEYS_NOT_LOADED = 0,
	KEYS_LOADING_IN_PROGRESS = 1,
	KEYS_LOADED = 2, // from here we can start the build operation
	HT_BUILD_IN_PROGRESS = 3,
	HT_BUILT_COMPLETED = 4
};

struct fact_data_t { // NOLINT
	inline void Initialize(idx_t chain_length_p, data_ptr_t chain_head_p, data_ptr_t *pointers_p, uint64_t *keys_p,
	                       idx_t ht_capacity_p) {
		chain_length = chain_length_p;
		chain_head = chain_head_p;
		pointers = pointers_p;
		keys = keys_p;

		ht_capacity = ht_capacity_p;
		// as the capacity is a power of 2, we can use a bitmask to get the hash
		ht_bitmask = ht_capacity - 1;
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

	// Builds the key map if it has not been built yet
	inline void BuildHT() {
		// if debug mode, check if ht is empty
#ifdef DEBUG
		for (idx_t idx = 0; idx < ht_capacity; idx++) {
			if (chain_ht[idx] != NULL_ELEMENT) {
				throw InternalException("Hash table is not empty!");
			}
		}
#endif

		FillHtWithIndex(keys, chain_length, chain_ht, ht_bitmask);
	}
};

} // namespace duckdb