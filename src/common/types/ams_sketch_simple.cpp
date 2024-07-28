#include "duckdb/common/types/ams_sketch_simple.hpp"

namespace duckdb {

inline uint8_t GetBitAtIndex(uint64_t hash, uint64_t bit) {
	return (hash >> bit) & 1;
}

inline uint8_t GetByteAtIndex(uint64_t hash, uint8_t byte_index) {
	return (hash >> (8 * byte_index)) & 0xFF;
}

template <uint64_t ArraySize, uint8_t NHashFunctions>
void AMSSketchSimple<ArraySize, NHashFunctions>::Update(uint64_t hash) {
	update_count++;

	for (uint8_t hash_function_index = 0; hash_function_index < NHashFunctions; hash_function_index++) {
		int8_t sign = 1 - 2 * GetBitAtIndex(hash, hash_function_index);

		uint8_t byte_index = hash_function_index + 1;
		uint8_t byte = GetByteAtIndex(hash, byte_index);

		// Calculate the index for the flat array
		uint64_t flat_index = hash_function_index * ArraySize + (byte % ArraySize);

		// Update the flat array
		flat_array[flat_index] += sign;
	}
}

// Explicit instantiation of the AMSSketchSimple template
template class AMSSketchSimple<128, 2>;
} // namespace duckdb
