#pragma once

#include <duckdb/common/vector.hpp>

namespace duckdb {

template<uint64_t ArraySize, uint8_t NHashFunctions>
class AMSSketchSimple {
public:
	explicit AMSSketchSimple() : flat_array(ArraySize * NHashFunctions),update_count(0) {}

	void Update(uint64_t hash);

	void Combine(const vector<int64_t>& other_flat_array);

	vector<int64_t> GetFlatArray() const {
		return flat_array;
	}

	vector<vector<int64_t>> GetArray() const {
		vector<vector<int64_t>> array(NHashFunctions, vector<int64_t>(ArraySize));
		for (uint8_t hash_function_index = 0; hash_function_index < NHashFunctions; hash_function_index++) {
			for (uint64_t index = 0; index < ArraySize; index++) {
				array[hash_function_index][index] = flat_array[hash_function_index * ArraySize + index];
			}
		}
		return array;
	}
	vector<int64_t> flat_array;          // Flat vector for sketch to improve NUMA locality
	uint64_t update_count;                    // Number of updates to the sketch
};

} // namespace duckdb