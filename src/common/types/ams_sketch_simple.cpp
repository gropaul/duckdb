#include "duckdb/common/types/ams_sketch_simple.hpp"

#include <cmath>
#include <duckdb/common/vector.hpp>

namespace duckdb {

AMSSketchSimple::AMSSketchSimple(uint64_t array_size) : array_size(array_size), update_count(0) {
	// Initialize the sketch array
	array = std::vector<int64_t>(array_size, 0);
}

uint64_t GetBitAtInt(uint64_t hash, uint64_t bit) {
	return (hash >> bit) & 1;
}

void AMSSketchSimple::Update(uint64_t hash, int64_t w) {
	update_count++;
	for (uint64_t i = 0; i < array_size; i++) {
		uint64_t bit = GetBitAtInt(hash, i);
		if (bit == 0) {
			array[i] -= w;
		} else {
			array[i] += w;
		}
	}
}

double AMSSketchSimple::Estimate() {
	// get the abs mean per upate
	double mean = 0;
	for (uint64_t i = 0; i < array_size; i++) {
		mean += abs(array[i]);
	}
	double result = mean / (double)(array_size * update_count);

	return result;
}

} // namespace duckdb
