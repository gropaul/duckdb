#include "duckdb/common/types/ams_sketch.hpp"
#include <cmath>

namespace duckdb {

FastAMS::FastAMS(uint64_t counters, uint64_t hashes)
    : m_seed(static_cast<uint64_t>(std::time(nullptr))), m_counters(counters) {
    m_p_filter = new int64_t[m_counters * hashes]();
    std::mt19937_64 rng(m_seed);
    std::uniform_int_distribution<uint64_t> dist;

    for (uint64_t i = 0; i < hashes; i++) {
        m_hash.push_back(dist(rng));
        m_fourwise_hash.push_back(dist(rng));
    }
}

FastAMS::FastAMS(const FastAMS &in)
    : m_seed(in.m_seed), m_counters(in.m_counters), m_hash(in.m_hash), m_fourwise_hash(in.m_fourwise_hash) {
    m_p_filter = new int64_t[m_counters * m_hash.size()];
    std::memcpy(m_p_filter, in.m_p_filter, m_counters * m_hash.size() * sizeof(int64_t));
}

FastAMS::FastAMS(const char *data) {
    uint64_t hashes;
    std::memcpy(&hashes, data, sizeof(uint64_t));
    data += sizeof(uint64_t);
    std::memcpy(&m_counters, data, sizeof(uint64_t));
    data += sizeof(uint64_t);
    std::memcpy(&m_seed, data, sizeof(uint64_t));
    data += sizeof(uint64_t);

    std::mt19937_64 rng(m_seed);
    for (uint64_t i = 0; i < hashes; i++) {
        m_hash.push_back(rng());
        m_fourwise_hash.push_back(rng());
    }

    m_p_filter = new int64_t[m_counters * hashes];
    std::memcpy(m_p_filter, data, m_counters * hashes * sizeof(int64_t));
}

FastAMS::~FastAMS() {
    delete[] m_p_filter;
}

FastAMS &FastAMS::operator=(const FastAMS &in) {
    if (this != &in) {
        if (m_counters != in.m_counters || m_hash.size() != in.m_hash.size()) {
            delete[] m_p_filter;
            m_p_filter = new int64_t[in.m_counters * in.m_hash.size()];
        }

        m_counters = in.m_counters;
        m_hash = in.m_hash;
        m_fourwise_hash = in.m_fourwise_hash;
        m_seed = in.m_seed;
        std::memcpy(m_p_filter, in.m_p_filter, m_counters * m_hash.size() * sizeof(int64_t));
    }

    return *this;
}

void FastAMS::Insert(uint64_t val) {
    for (uint64_t i = 0; i < m_hash.size(); i++) {
        uint64_t h = m_hash[i] % m_counters;
        uint64_t m = m_fourwise_hash[i];

        if ((m & 1) == 1) {
            m_p_filter[i * m_counters + h] += val;
        } else {
            m_p_filter[i * m_counters + h] -= val;
        }
    }
}

void FastAMS::Clear() {
    std::memset(m_p_filter, 0, m_counters * m_hash.size() * sizeof(int64_t));
}

template <typename T>
T FastAMS::GetMedian(const std::multiset<T> &data) const {
    auto it = data.begin();
    std::advance(it, data.size() / 2);
    return *it;
}

double FastAMS::Estimate() const {
    std::multiset<double> estimates;
    for (uint64_t i = 0; i < m_hash.size(); i++) {
        double sum = 0;
        for (uint64_t j = 0; j < m_counters; j++) {
            sum += std::pow(m_p_filter[i * m_counters + j], 2);
        }
        estimates.insert(sum);
    }
    return std::sqrt(GetMedian(estimates));
}

// std::ostream &operator<<(std::ostream &os, const FastAMS &s) {
//     os << "\n\nFast AMS Sketch:\n";
//     os << "m_hash size: " << static_cast<unsigned long long>(s.m_hash.size()) << "\n";
//     os << "m_counters: " << static_cast<unsigned long long>(s.m_counters) << "\n";
//
//     os << "m_hash: ";
//     for (uint64_t i = 0; i < s.m_hash.size(); i++) {
//         os << " " << s.m_hash[i];
//     }
//     os << "\n";
//
//     os << "m_fourwise_hash: ";
//     for (uint64_t i = 0; i < s.m_fourwise_hash.size(); i++) {
//         os << " " << s.m_fourwise_hash[i];
//     }
//     os << "\n";
//
//     os << "m_p_filter: ";
//     for (uint64_t i = 0; i < s.m_counters * s.m_hash.size(); i++) {
//         os << " " << s.m_p_filter[i];
//     }
//     os << "\n";
//
//     return os;
// }

} // namespace duckdb