#pragma once

#include <duckdb/common/vector.hpp>

namespace duckdb {

// AMS Sketch Class
// AMS Sketch Class
class AMSSketchSimple {
public:
	explicit AMSSketchSimple(uint64_t array_size, uint8_t n_hash_functions);

	void Update(uint64_t hash);

	void Combine(const AMSSketchSimple &other);

	vector<vector<int64_t>> GetArray() {
		return array;
	}

private:
	uint64_t array_size;           // Size of the sketch array
	uint8_t n_hash_functions;      // Number of hash functions
	uint64_t update_count;         // Number of updates to the sketch
	vector<vector<int64_t>> array; // Sketch 2d array
};

} // namespace duckdb