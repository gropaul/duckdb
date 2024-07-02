#pragma once

#include <duckdb/common/vector.hpp>

namespace duckdb {

// AMS Sketch Class
// AMS Sketch Class
class AMSSketchSimple {
public:
    explicit AMSSketchSimple(uint64_t array_size);

    void Update(uint64_t hash, int64_t w);

    double Estimate();

private:
	uint64_t array_size; // Size of the sketch array
	uint64_t update_count; // Number of updates to the sketch
    std::vector<int64_t> array;
};


} // namespace duckdb