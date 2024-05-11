#pragma once

#include "memory.h"

namespace duckdb {

inline void Swap(uint64_t &a, uint64_t &b) {
	uint64_t t = a;
	a = b;
	b = t;
}

inline void Swap(data_ptr_t &a, data_ptr_t &b) {
	data_ptr_t t = a;
	a = b;
	b = t;
}

// The partition function rearranges the elements based on pivot element
inline idx_t Partition(uint64_t *keys, data_ptr_t *pointers, idx_t low, idx_t high) {
	uint64_t pivot = keys[high]; // pivot
	idx_t i = (low - 1); // Index of smaller element

	for (idx_t j = low; j <= high - 1; j++) {
		// If current element is smaller than or equal to pivot
		if (keys[j] <= pivot) {
			i++; // increment index of smaller element
			Swap(keys[i], keys[j]);
			Swap(pointers[i], pointers[j]);
		}
	}
	Swap(keys[i + 1], keys[high]);
	Swap(pointers[i + 1], pointers[high]);
	return (i + 1);
}

inline void QuickSort(uint64_t *keys, data_ptr_t *pointers, idx_t low, idx_t high) {
	if (low < high) {
		idx_t pi = Partition(keys, pointers, low, high);

		// Only call QuickSort on the left part if there are elements to sort
		if (pi > 0) { // This check ensures we don't call QuickSort with pi-1 when pi is 0
			QuickSort(keys, pointers, low, pi - 1);
		}

		QuickSort(keys, pointers, pi + 1, high);
	}
}

inline void SortByKeys(uint64_t *keys, data_ptr_t *pointers, idx_t chain_length) {
	// sort pointers and keys by keys
	QuickSort(keys, pointers, 0, chain_length - 1);
}

struct fact_data_t { // NOLINT
	void Initialize(idx_t chain_length_p, data_ptr_t chain_head_p, data_ptr_t *pointers_p, uint64_t *keys_p) {
		chain_length = chain_length_p;
		chain_head = chain_head_p;
		pointers = pointers_p;
		keys = keys_p;
		map_built = false;
		keys_gathered = false;

		// initialize the key map
		// key_map = make_uniq<unordered_map<uint64_t, PointerOccurrence>>();
		// auto _key_map = make_uniq<unordered_map<uint64_t, PointerOccurrence>>();
		// key_map = std::make_unique<unordered_map<uint64_t, PointerOccurrence>>();
	}

	uint64_t chain_length;
	data_ptr_t chain_head;

	// the pointers that form the original chain
	data_ptr_t *pointers;
	// the keys for the intersection
	uint64_t *keys;
	// whether the map has been built
	bool map_built;
	// keys gathered from the chain
	bool keys_gathered;

	bool KeyMapBuilt() {
		return map_built;
	}

	void SortKeys() {
		SortByKeys(keys, pointers, chain_length);
	}
};

} // namespace duckdb