#include "duckdb/common/types/ams_sketch.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace duckdb {
template <typename T>
T FastAMS::GetMedian(const std::multiset<T> &data) {
	auto it = data.begin();
	std::advance(it, data.size() / 2);
	return *it;
}

using byte = char;

FastAMS::FastAMS(uint32_t counters, uint32_t hashes)
    : m_seed(static_cast<uint32_t>(std::time(nullptr))), m_counters(counters) {
	m_p_filter = new int64_t[m_counters * hashes];
	std::fill(m_p_filter, m_p_filter + static_cast<size_t>(m_counters * hashes), 0); // equivalent to bzero

	MyRandomGenerator rng(m_seed);                // Mersenne Twister random number generator
	std::uniform_int_distribution<uint32_t> dist; // Uniform distribution

	for (uint32_t i = 0; i < hashes; i++) {
		m_hash.push_back(dist(rng));          // Generate a random number for hash
		m_fourwise_hash.push_back(dist(rng)); // Generate a random number for fourwise hash
	}
}

FastAMS::FastAMS(uint32_t counters, uint32_t hashes, uint32_t seed) : m_seed(seed), m_counters(counters) {
	m_p_filter = new int64_t[m_counters * hashes];
	std::fill(m_p_filter, m_p_filter + static_cast<size_t>(m_counters * hashes), 0);

	MyRandomGenerator rng(seed);

	for (uint32_t i = 0; i < hashes; i++) {
		m_hash.push_back(rng());
		m_fourwise_hash.push_back(rng());
	}
}

FastAMS::FastAMS(const FastAMS &in)
    : m_seed(in.m_seed), m_counters(in.m_counters), m_hash(in.m_hash), m_fourwise_hash(in.m_fourwise_hash) {
	m_p_filter = new int64_t[m_counters * m_hash.size()];
	std::memcpy(m_p_filter, in.m_p_filter, m_counters * m_hash.size() * sizeof(int32_t));
}

FastAMS::FastAMS(const byte *data) {
	uint32_t hashes;
	std::memcpy(&hashes, data, sizeof(uint32_t));
	data += sizeof(uint32_t);
	std::memcpy(&m_counters, data, sizeof(uint32_t));
	data += sizeof(uint32_t);
	std::memcpy(&m_seed, data, sizeof(uint32_t));
	data += sizeof(uint32_t);

	MyRandomGenerator rng(m_seed);
	for (uint32_t i = 0; i < hashes; i++) {
		m_hash.push_back(rng());
		m_fourwise_hash.push_back(rng());
	}

	m_p_filter = new int64_t[m_counters * hashes];
	std::memcpy(m_p_filter, data, static_cast<uint32_t>(m_counters * hashes) * sizeof(int32_t));
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
		std::memcpy(m_p_filter, in.m_p_filter, m_counters * m_hash.size() * sizeof(int32_t));
	}

	return *this;
}

void FastAMS::Insert(uint64_t val) {
	for (uint32_t i = 0; i < m_hash.size(); i++) {
		uint32_t h = m_hash[i] % m_counters;
		uint32_t m = m_fourwise_hash[i];

		if ((m & 1) == 1) {
			m_p_filter[i * m_counters + h] += val;
		} else {
			m_p_filter[i * m_counters + h] -= val;
		}
	}
}

void FastAMS::Erase(const std::string &id, int32_t val) {
	uint64_t l = std::atoll(id.c_str());
	Erase(l, val);
}

void FastAMS::Erase(const UniversalHash::value_type &id, int32_t val) {

	for (uint32_t i = 0; i < m_hash.size(); i++) {
		uint32_t h = m_hash[i] % m_counters;
		uint32_t m = m_fourwise_hash[i];

		if ((m & 1) == 1) {
			m_p_filter[i * m_counters + h] -= val;
		} else {
			m_p_filter[i * m_counters + h] += val;
		}
	}
}

void FastAMS::Clear() {
	std::fill(m_p_filter, m_p_filter + m_counters * m_hash.size(), 0);
}

int32_t FastAMS::GetFrequency(const std::string &id) {
	uint64_t l = std::atoll(id.c_str());
	return GetFrequency(l);
}

int32_t FastAMS::GetFrequency(const UniversalHash::value_type &id) {
	std::multiset<int32_t> answer;

	for (uint64_t i = 0; i < m_hash.size(); i++) {
		uint64_t h = m_hash[i] % m_counters;
		uint64_t m = m_fourwise_hash[i];

		if ((m & 1) == 1) {
			answer.insert(m_p_filter[i * m_counters + h]);
		} else {
			answer.insert(-m_p_filter[i * m_counters + h]);
		}
	}

	return GetMedian<int32_t>(answer);
}

uint32_t FastAMS::GetVectorLength() const {
	return m_counters;
}

uint32_t FastAMS::GetNumberOfHashes() const {
	return m_hash.size();
}

uint32_t FastAMS::GetSize() const {
	return 3 * sizeof(uint32_t) + m_counters * m_hash.size() * sizeof(int32_t);
}

void FastAMS::GetData(byte **data, uint32_t &length) const {
	length = GetSize();
	*data = new byte[length];
	byte *p = *data;

	uint32_t l = m_hash.size();
	std::memcpy(p, &l, sizeof(uint32_t));
	p += sizeof(uint32_t);
	std::memcpy(p, &m_counters, sizeof(uint32_t));
	p += sizeof(uint32_t);
	std::memcpy(p, &m_seed, sizeof(uint32_t));
	p += sizeof(uint32_t);
	std::memcpy(p, m_p_filter, m_counters * m_hash.size() * sizeof(int32_t));
	p += m_counters * m_hash.size() * sizeof(int32_t);

	assert(p == (*data) + length);
}

std::ostream &operator<<(std::ostream &os, const FastAMS &s) {
	os << s.m_hash.size() << " " << s.m_counters;

	for (uint32_t i = 0; i < s.m_hash.size(); i++) {
		os << " " << s.m_hash[i];
	}

	for (uint32_t i = 0; i < s.m_fourwise_hash.size(); i++) {
		os << " " << s.m_fourwise_hash[i];
	}

	for (uint32_t i = 0; i < s.m_counters * s.m_hash.size(); i++) {
		os << " " << s.m_p_filter[i];
	}

	return os;
}
} // namespace duckdb