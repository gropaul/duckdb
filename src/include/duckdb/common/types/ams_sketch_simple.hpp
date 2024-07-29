#pragma once


namespace duckdb {

template<uint64_t ArraySize, uint8_t NHashFunctions>
class AMSSketchSimple {
public:

	// as ArraySize is a power of 2, we can use a bitmask instead of modulo
	static constexpr uint64_t ARRAY_BITMASK = ArraySize - 1;

	explicit AMSSketchSimple() : update_count(0) {
		// Initialize the flat_array to zero
		for (uint64_t i = 0; i < ArraySize * NHashFunctions; i++) {
			flat_array[i] = 0;
		}

		// the array size must be a power of 2
		static_assert((ArraySize & (ArraySize - 1)) == 0, "ArraySize must be a power of 2");
	}

	inline void Update(uint64_t hash);

	void Combine(AMSSketchSimple<ArraySize, NHashFunctions>& other) {
		for (uint64_t i = 0; i < ArraySize * NHashFunctions; i++) {
			flat_array[i] += other.flat_array[i];
		}
		this->update_count += other.update_count;
	}

	// only used for logging, will not be used in the actual implementation
	vector<vector<int64_t>> GetArray() const {
		vector<vector<int64_t>> array(NHashFunctions, vector<int64_t>(ArraySize));
		for (uint8_t hash_function_index = 0; hash_function_index < NHashFunctions; hash_function_index++) {
			for (uint64_t index = 0; index < ArraySize; index++) {
				array[hash_function_index][index] = flat_array[hash_function_index * ArraySize + index];
			}
		}
		return array;
	}

private:
	int64_t flat_array[ArraySize * NHashFunctions];
	uint64_t update_count;
};

} // namespace duckdb
