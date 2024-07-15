#include "duckdb/common/types/ams_sketch_simple.hpp"

namespace duckdb {

inline uint64_t GetBitAtIndex(uint64_t hash, uint64_t bit) {
	return (hash >> bit) & 1;
}

// Function to split a uint64_t into an array of uint8_t
std::array<uint8_t, 8> SplitUint64(uint64_t value) {
	std::array<uint8_t, 8> result;
	for (int i = 0; i < 8; ++i) {
		// Shift the value right by 8*i bits and mask out the relevant byte
		result[7 - i] = (value >> (8 * i)) & 0xFF;
	}
	return result;
}

inline uint8_t GetByteAtIndex(uint64_t hash, uint8_t byte_index) {
	return (hash >> (8 * byte_index)) & 0xFF;
}

AMSSketchSimple::AMSSketchSimple(uint64_t array_size, uint8_t n_hash_functions)
    : array_size(array_size), n_hash_functions(n_hash_functions), update_count(0) {

	// we use the separate bytes of the hash functions as hash values, so array_size must be smaller than 256
	D_ASSERT(array_size <= 256);

	// there are only 8 byte, first byte is used for the sign bit, so we can only use 7 bytes
	D_ASSERT(n_hash_functions <= 7);

	// Initialize the sketch array as n_hash_functions x array_size matrix
	array = vector<vector<int64_t>>(n_hash_functions, vector<int64_t>(array_size, 0));
}

void AMSSketchSimple::Update(uint64_t hash) {
	update_count++;

	int8_t sign = GetBitAtIndex(hash, 0) == 0 ? -1 : 1;

	for (uint8_t hash_function_index = 0; hash_function_index < n_hash_functions; hash_function_index++) {

		uint8_t byte_index = hash_function_index + 1;
		uint8_t byte = GetByteAtIndex(hash, byte_index);

		// get the index of the array
		uint64_t index = byte % array_size;

		// update the array
		array[hash_function_index][index] += sign;
	}
}

} // namespace duckdb
