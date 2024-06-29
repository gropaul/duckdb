#pragma once

#include <duckdb/common/vector.hpp>

namespace duckdb {

// AMS Sketch Class
// AMS Sketch Class
class AMSSketch {
public:
    AMSSketch(int d, int t, int p);

    // Hash function h_j that maps input domain to {1, 2, ..., t}
    int HashH(uint64_t i, int j, int t);

    // Hash function g_j that maps elements to {-1, +1}
    int HashG(uint64_t i, int j, int p);

    void Update(uint64_t i, int64_t w);

    double Estimate();

private:
    int d, t, p; // d: number of hash functions, t: number of buckets, p: prime field
    std::vector<std::vector<int64_t>> c; // Sketch array
};


} // namespace duckdb