#pragma once
#include <vector>
#include <set>
#include <string>

namespace duckdb {

using byte = char;

class MyRandomGenerator {
public:
    using result_type = uint64_t;

    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return std::numeric_limits<result_type>::max(); }

    explicit MyRandomGenerator(uint64_t seed) : m_seed(seed) {}

    result_type operator()() {
        // Custom random number generation algorithm
        // Example: Linear Congruential Generator
        m_seed = (m_seed * 1664525 + 1013904223) & 0xFFFFFFFF;
        return m_seed;
    }

private:
    uint64_t m_seed;
};

class UniversalHash {
public:
    using value_type = uint64_t;

    explicit UniversalHash(MyRandomGenerator& rng);

    uint64_t Hash(value_type val);

private:
    MyRandomGenerator& m_rng;
};



class FastAMS {
public:
    FastAMS(uint64_t counters, uint64_t hashes);
    FastAMS(uint64_t counters, uint64_t hashes, uint64_t seed);
    FastAMS(const FastAMS& in);
    explicit FastAMS(const byte* data);
    ~FastAMS();
    FastAMS& operator=(const FastAMS& in);

    void Insert(uint64_t val);
    void Erase(const std::string& id, int64_t val);
    void Erase(const UniversalHash::value_type& id, int64_t val);
    void Clear();
    int64_t GetFrequency(const std::string& id);
    int64_t GetFrequency(const UniversalHash::value_type& id);
    uint64_t GetVectorLength() const;
    uint64_t GetNumberOfHashes() const;
    uint64_t GetSize() const;
    void GetData(byte** data, uint64_t& length) const;
    template <typename T>
    T GetMedian(const std::multiset<T> &data);

    friend std::ostream& operator<<(std::ostream& os, const FastAMS& s);

    std::string Print();

private:
    uint64_t m_seed;
    uint64_t m_counters;
    int64_t* m_p_filter;
    std::vector<uint64_t> m_hash;
    std::vector<uint64_t> m_fourwise_hash;
};


} // namespace duckdb

