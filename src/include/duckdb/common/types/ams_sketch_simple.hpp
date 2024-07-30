#pragma once


namespace duckdb {

inline uint8_t ShiftToByteIndex(uint64_t hash, uint8_t byte_index) {
	return static_cast<uint8_t>(hash >> (byte_index * 8));
}

inline uint8_t ShiftByteToBitIndex(uint8_t byte, uint8_t bit_index) {
	return (byte >> bit_index) & 1;
}

template<uint8_t ArraySize, uint8_t NHashFunctions>
class AMSSketchSimple {
public:

	// as ArraySize is a power of 2, we can use a bitmask instead of modulo
	static constexpr uint8_t ARRAY_BITMASK = ArraySize - 1;

	int64_t flat_array[ArraySize * NHashFunctions];
	uint64_t update_count;

	explicit AMSSketchSimple() : update_count(0) {
		// Initialize the flat_array to zero
		for (uint16_t i = 0; i < ArraySize * NHashFunctions; i++) {
			flat_array[i] = 0;
		}

		// the array size must be a power of 2
		static_assert((ArraySize & (ArraySize - 1)) == 0, "ArraySize must be a power of 2");
	}

	inline void Update(uint64_t hash){
		update_count++;

		for (uint8_t hash_function_index = 0; hash_function_index < NHashFunctions; hash_function_index++) {
			uint8_t byte = ShiftToByteIndex(hash, hash_function_index);
			uint8_t increment = ShiftByteToBitIndex(byte, 7);
			int8_t sign = 1 - 2 * increment;

			uint8_t byte_offset = byte & this->ARRAY_BITMASK;

			// Max size is 8 * 128 = 2048, so we can use uint16_t
			uint16_t flat_index = hash_function_index * ArraySize + byte_offset;
			flat_array[flat_index] += sign;
		}
	}

	void Combine(AMSSketchSimple<ArraySize, NHashFunctions>& other) {
		for (uint16_t i = 0; i < ArraySize * NHashFunctions; i++) {
			flat_array[i] += other.flat_array[i];
		}
		this->update_count += other.update_count;
	}

	// only used for logging, will not be used in the actual implementation
	vector<vector<int64_t>> GetArray() const {
		vector<vector<int64_t>> array(NHashFunctions, vector<int64_t>(ArraySize));
		for (uint8_t hash_function_index = 0; hash_function_index < NHashFunctions; hash_function_index++) {
			for (uint8_t index = 0; index < ArraySize; index++) {
				uint16_t flat_index = hash_function_index * ArraySize + index;
				array[hash_function_index][index] = flat_array[flat_index];
			}
		}
		return array;
	}
};

} // namespace duckdb
