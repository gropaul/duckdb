#include "duckdb/common/types/ams_sketch_simple.hpp"

#include <cmath>
#include <duckdb/common/vector.hpp>

namespace duckdb {

AMSSketchSimple::AMSSketchSimple(uint64_t array_size) : array_size(array_size) {
	// Initialize the sketch array
	array = std::vector<int64_t>(array_size, 0);
}


uint64_t GetBitAtInt(uint64_t hash, uint64_t bit) {
	return (hash >> bit) & 1;
}

void AMSSketchSimple::Update(uint64_t hash, int64_t w) {
	for (uint64_t i = 0; i < array_size; i++) {
		uint64_t bit = GetBitAtInt(hash, i);
		if (bit == 0) {
			array[i] -= w;
		} else {
			array[i] += w;
		}
	}
}

// calculate the standard deviation of an array, will abs all values
double StandardDeviation(uint64_t array_size, std::vector<int64_t >& vector) {


	if (array_size == 0) {
		return 0.0; // Handle empty array case
	}

	// the array can have negative values, so abs all
	for (uint64_t i = 0; i < array_size; i++) {
		vector[i] = std::abs(vector[i]);
	}

	// Calculate the mean (average) of the elements in the array
	double mean = 0.0;
	for (uint64_t value : vector) {
		mean += value;
	}
	mean /= array_size;

	// Calculate the variance
	double variance = 0.0;
	for (uint64_t value : vector) {
		variance += std::pow(value - mean, 2);
	}
	variance /= array_size;

	// Calculate the standard deviation
	return std::sqrt(variance);
}

double AMSSketchSimple::Estimate() {
	return StandardDeviation(array_size, array);
}

} // namespace duckdb
