#include "duckdb/common/types/ams_sketch.hpp"

#include <cmath>
#include <duckdb/common/vector.hpp>
#include <algorithm>

namespace duckdb {

AMSSketch::AMSSketch(int d, int t, int p) : d(d), t(t), p(p) {
	// Initialize the sketch array
	c = std::vector<std::vector<int64_t>>(d, std::vector<int64_t>(t, 0));
	srand(time(0)); // Seed the random number generator
}

// Hash function h_j that maps input domain to {1, 2, ..., t}
int AMSSketch::HashH(uint64_t i, int j, int t) {
	// Simple hash function, should be replaced with a proper hash function
	return (i * j + j) % t;
}

// Hash function g_j that maps elements to {-1, +1}
int AMSSketch::HashG(uint64_t i, int j, int p) {
	// The hash function coefficients should depend on j to ensure different hash functions for each row
	int64_t a = (rand() + j) % p;
	int64_t b = (rand() + j) % p;
	int64_t c = (rand() + j) % p;
	int64_t d = (rand() + j) % p;
	uint64_t result = (a * i * i * i + b * i * i + c * i + d) % p;
	return 2 * (result % 2) - 1;
}

void AMSSketch::Update(uint64_t i, int64_t w) {
	for (int j = 0; j < d; ++j) {
		int hj = HashH(i, j, t);
		int gj = HashG(i, j, p);
		c[j][hj] += w * gj;
	}
}

double AMSSketch::Estimate() {
	vector<double> estimates(d, 0);
	for (int j = 0; j < d; ++j) {
		for (int k = 0; k < t; ++k) {
			estimates[j] += pow(c[j][k], 2);
		}
	}
	sort(estimates.begin(), estimates.end());
	return sqrt(estimates[d / 2]);
}
} // namespace duckdb
