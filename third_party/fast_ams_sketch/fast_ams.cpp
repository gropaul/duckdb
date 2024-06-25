#include "fast_ams.hpp"
#include <iostream>
#include <ctime>
#include <vector>
#include <random>
#include <cstring>
#include <cassert>
#include <set>
#include <string>
#include <sstream>
#include <cstdlib>

namespace duckdb_fastams {
template<typename T>
T getMedian(std::multiset<T>& data) {
    auto it = data.begin();
    std::advance(it, data.size() / 2);
    return *it;
}

using byte = char;

FastAMS::FastAMS(unsigned long counters, unsigned long hashes)
    : m_seed(static_cast<unsigned long>(std::time(0))), m_counters(counters) {
    m_pFilter = new long[m_counters * hashes];
    std::fill(m_pFilter, m_pFilter + m_counters * hashes, 0); // equivalent to bzero

    MyRandomGenerator rng(m_seed); // Mersenne Twister random number generator
    std::uniform_int_distribution<unsigned long> dist; // Uniform distribution

    for (unsigned long i = 0; i < hashes; i++) {
        m_hash.push_back(dist(rng)); // Generate a random number for hash
        m_fourwiseHash.push_back(dist(rng)); // Generate a random number for fourwise hash
    }
}

FastAMS::FastAMS(
    unsigned long counters,
    unsigned long hashes,
    unsigned long seed
) : m_seed(seed), m_counters(counters) {
    m_pFilter = new long[m_counters * hashes];
    std::fill(m_pFilter, m_pFilter + m_counters * hashes, 0);

    MyRandomGenerator rng(seed);

    for (unsigned long i = 0; i < hashes; i++) {
        m_hash.push_back(rng());
        m_fourwiseHash.push_back(rng());
    }
}

FastAMS::FastAMS(
    const FastAMS& in
) : m_seed(in.m_seed),
    m_counters(in.m_counters),
    m_hash(in.m_hash),
    m_fourwiseHash(in.m_fourwiseHash) {
    m_pFilter = new long[m_counters * m_hash.size()];
    std::memcpy(m_pFilter, in.m_pFilter, m_counters * m_hash.size() * sizeof(long));
}

FastAMS::FastAMS(const byte* data) {
    unsigned long hashes;
    std::memcpy(&hashes, data, sizeof(unsigned long));
    data += sizeof(unsigned long);
    std::memcpy(&m_counters, data, sizeof(unsigned long));
    data += sizeof(unsigned long);
    std::memcpy(&m_seed, data, sizeof(unsigned long));
    data += sizeof(unsigned long);

    MyRandomGenerator rng(m_seed);
    for (unsigned long i = 0; i < hashes; i++) {
        m_hash.push_back(rng());
        m_fourwiseHash.push_back(rng());
    }

    m_pFilter = new long[m_counters * hashes];
    std::memcpy(m_pFilter, data, m_counters * hashes * sizeof(long));
}

FastAMS::~FastAMS() {
    delete[] m_pFilter;
}

FastAMS& FastAMS::operator=(const FastAMS& in) {
    if (this != &in) {
        if (m_counters != in.m_counters || m_hash.size() != in.m_hash.size()) {
            delete[] m_pFilter;
            m_pFilter = new long[in.m_counters * in.m_hash.size()];
        }

        m_counters = in.m_counters;
        m_hash = in.m_hash;
        m_fourwiseHash = in.m_fourwiseHash;
        m_seed = in.m_seed;
        std::memcpy(m_pFilter, in.m_pFilter, m_counters * m_hash.size() * sizeof(long));
    }

    return *this;
}

void FastAMS::insert(const std::string& id, long val) {
    unsigned long long l = std::atoll(id.c_str());
    insert(l, val);
}

void FastAMS::insert(
    const UniversalHash::value_type& id,
    long val
) {
    MyRandomGenerator rng(m_seed);

    for (unsigned long i = 0; i < m_hash.size(); i++) {
        unsigned long h = m_hash[i] % m_counters;
        unsigned long m = m_fourwiseHash[i];

        if ((m & 1) == 1)
            m_pFilter[i * m_counters + h] += val;
        else
            m_pFilter[i * m_counters + h] -= val;
    }
}

void FastAMS::erase(const std::string& id, long val) {
    unsigned long long l = std::atoll(id.c_str());
    erase(l, val);
}

void FastAMS::erase(
    const UniversalHash::value_type& id,
    long val
) {
    MyRandomGenerator rng(m_seed);

    for (unsigned long i = 0; i < m_hash.size(); i++) {
        unsigned long h = m_hash[i] % m_counters;
        unsigned long m = m_fourwiseHash[i];

        if ((m & 1) == 1)
            m_pFilter[i * m_counters + h] -= val;
        else
            m_pFilter[i * m_counters + h] += val;
    }
}

void FastAMS::clear() {
    std::fill(m_pFilter, m_pFilter + m_counters * m_hash.size(), 0);
}

long FastAMS::getFrequency(const std::string& id) const {
    unsigned long long l = std::atoll(id.c_str());
    return getFrequency(l);
}

long FastAMS::getFrequency(
    const UniversalHash::value_type& id
) const {
    std::multiset<long> answer;

    for (unsigned long i = 0; i < m_hash.size(); i++) {
        unsigned long h = m_hash[i] % m_counters;
        unsigned long m = m_fourwiseHash[i];

        if ((m & 1) == 1)
            answer.insert(m_pFilter[i * m_counters + h]);
        else
            answer.insert(-m_pFilter[i * m_counters + h]);
    }

    return getMedian<long>(answer);
}

unsigned long FastAMS::getVectorLength() const {
    return m_counters;
}

unsigned long FastAMS::getNumberOfHashes() const {
    return m_hash.size();
}

unsigned long FastAMS::getSize() const {
    return 3 * sizeof(unsigned long) +
        m_counters * m_hash.size() * sizeof(long);
}

void FastAMS::getData(byte** data, unsigned long& length) const {
    length = getSize();
    *data = new byte[length];
    byte* p = *data;

    unsigned long l = m_hash.size();
    std::memcpy(p, &l, sizeof(unsigned long));
    p += sizeof(unsigned long);
    std::memcpy(p, &m_counters, sizeof(unsigned long));
    p += sizeof(unsigned long);
    std::memcpy(p, &m_seed, sizeof(unsigned long));
    p += sizeof(unsigned long);
    std::memcpy(p, m_pFilter, m_counters * m_hash.size() * sizeof(long));
    p += m_counters * m_hash.size() * sizeof(long);

    assert(p == (*data) + length);
}

std::ostream& operator<<(
    std::ostream& os,
    const FastAMS& s
) {
    os << s.m_hash.size() << " " << s.m_counters;

    for (unsigned long i = 0; i < s.m_hash.size(); i++)
        os << " " << s.m_hash[i];

    for (unsigned long i = 0; i < s.m_fourwiseHash.size(); i++)
        os << " " << s.m_fourwiseHash[i];

    for (unsigned long i = 0; i < s.m_counters * s.m_hash.size(); i++)
        os << " " << s.m_pFilter[i];

    return os;
}

} // namespace duckdb_fastams