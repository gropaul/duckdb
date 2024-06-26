#pragma once
#include <vector>
#include <set>
#include <string>

namespace duckdb {

using byte = char;

class MyRandomGenerator {
public:
    using result_type = uint32_t;

    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return std::numeric_limits<result_type>::max(); }

    explicit MyRandomGenerator(uint32_t seed) : m_seed(seed) {}

    result_type operator()() {
        // Custom random number generation algorithm
        // Example: Linear Congruential Generator
        m_seed = (m_seed * 1664525 + 1013904223) & 0xFFFFFFFF;
        return m_seed;
    }

private:
    uint32_t m_seed;
};

class UniversalHash {
public:
    using value_type = uint32_t;

    explicit UniversalHash(MyRandomGenerator& rng);

    uint32_t Hash(value_type val);

private:
    MyRandomGenerator& m_rng;
};



class FastAMS {
public:
    FastAMS(uint32_t counters, uint32_t hashes);
    FastAMS(uint32_t counters, uint32_t hashes, uint32_t seed);
    FastAMS(const FastAMS& in);
    explicit FastAMS(const byte* data);
    ~FastAMS();
    FastAMS& operator=(const FastAMS& in);

    void Insert(uint64_t val);
    void Erase(const std::string& id, int32_t val);
    void Erase(const UniversalHash::value_type& id, int32_t val);
    void Clear();
    int32_t GetFrequency(const std::string& id);
    int32_t GetFrequency(const UniversalHash::value_type& id);
    uint32_t GetVectorLength() const;
    uint32_t GetNumberOfHashes() const;
    uint32_t GetSize() const;
    void GetData(byte** data, uint32_t& length) const;
    template <typename T>
    T GetMedian(const std::multiset<T> &data);

    friend std::ostream& operator<<(std::ostream& os, const FastAMS& s);

private:
    uint64_t m_seed;
    uint64_t m_counters;
    int64_t* m_p_filter;
    std::vector<uint64_t> m_hash;
    std::vector<uint64_t> m_fourwise_hash;
};


} // namespace duckdb

