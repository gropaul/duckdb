#pragma once

#include <vector>
#include <random>
#include <set>
#include <cstdint>

namespace duckdb {

class FastAMS {
public:
    FastAMS(uint64_t counters, uint64_t hashes);
    FastAMS(const FastAMS &in);
    FastAMS(const char *data);
    ~FastAMS();

    FastAMS &operator=(const FastAMS &in);

    void Insert(uint64_t val);
    int64_t GetFrequency(const std::string &id);
    void Clear();
    void GetData(char **data, uint64_t &length) const;
    uint64_t GetVectorLength() const;
    uint64_t GetNumberOfHashes() const;
    uint64_t GetSize() const;
    double Estimate() const;

    // friend std::ostream &operator<<(std::ostream &os, const FastAMS &s);

private:
    uint64_t m_seed;
    uint64_t m_counters;
    std::vector<uint64_t> m_hash;
    std::vector<uint64_t> m_fourwise_hash;
    int64_t *m_p_filter;

    template <typename T>
    T GetMedian(const std::multiset<T> &data) const;
};

} // namespace duckdb