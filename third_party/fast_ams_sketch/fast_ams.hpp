#include <vector>
#include <cstring>
#include <cassert>
#include <set>
#include <string>
#include <sstream>
#include <cstdlib>

namespace duckdb_fastams {


using byte = char;

class MyRandomGenerator {
public:
    using result_type = unsigned long;

    static constexpr result_type min() { return 0; }
    static constexpr result_type max() { return std::numeric_limits<result_type>::max(); }

    MyRandomGenerator(unsigned long seed) : m_seed(seed) {}

    result_type operator()() {
        // Custom random number generation algorithm
        // Example: Linear Congruential Generator
        m_seed = (m_seed * 1664525 + 1013904223) & 0xFFFFFFFF;
        return m_seed;
    }

private:
    unsigned long m_seed;
};

class UniversalHash {
public:
    using value_type = unsigned long;

    UniversalHash(MyRandomGenerator& rng);

    unsigned long hash(value_type val);

private:
    MyRandomGenerator& m_rng;
};

template<typename T>
T getMedian(std::multiset<T>& data);

class FastAMS {
public:
    FastAMS(unsigned long counters, unsigned long hashes);
    FastAMS(unsigned long counters, unsigned long hashes, unsigned long seed);
    FastAMS(const FastAMS& in);
    FastAMS(const byte* data);
    ~FastAMS();
    FastAMS& operator=(const FastAMS& in);

    void insert(const std::string& id, long val);
    void insert(const UniversalHash::value_type& id, long val);
    void erase(const std::string& id, long val);
    void erase(const UniversalHash::value_type& id, long val);
    void clear();
    long getFrequency(const std::string& id) const;
    long getFrequency(const UniversalHash::value_type& id) const;
    unsigned long getVectorLength() const;
    unsigned long getNumberOfHashes() const;
    unsigned long getSize() const;
    void getData(byte** data, unsigned long& length) const;

    friend std::ostream& operator<<(std::ostream& os, const FastAMS& s);

private:
    unsigned long m_seed;
    unsigned long m_counters;
    long* m_pFilter;
    std::vector<unsigned long> m_hash;
    std::vector<unsigned long> m_fourwiseHash;
};

} // namespace duckdb_fastams