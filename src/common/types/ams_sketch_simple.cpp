#include "duckdb/common/types/ams_sketch_simple.hpp"

namespace duckdb {

inline uint64_t GetBitAtIndex(uint64_t hash, uint64_t bit) {
	return (hash >> bit) & 1;
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


void AMSSketchSimple::Combine(const duckdb::AMSSketchSimple &other) {
	D_ASSERT(array_size == other.array_size);
	D_ASSERT(n_hash_functions == other.n_hash_functions);

	for (uint8_t hash_function_index = 0; hash_function_index < n_hash_functions; hash_function_index++) {
		for (uint64_t index = 0; index < array_size; index++) {
			array[hash_function_index][index] += other.array[hash_function_index][index];
		}
	}
}

void AMSSketchSimple::Update(uint64_t hash) {
	update_count++;


	for (uint8_t hash_function_index = 0; hash_function_index < n_hash_functions; hash_function_index++) {

		int8_t sign = GetBitAtIndex(hash, hash_function_index) == 0 ? -1 : 1;

		uint8_t byte_index = hash_function_index + 1;
		uint8_t byte = GetByteAtIndex(hash, byte_index);

		// get the index of the array
		uint64_t index = byte % array_size;

		// update the array
		array[hash_function_index][index] += sign;
	}
}

} // namespace duckdb
