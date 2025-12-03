#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static constexpr idx_t MAX_NUM_SECTORS = (1ULL << 26);
static constexpr idx_t MIN_NUM_BITS_PER_KEY = 12;
static constexpr idx_t MIN_NUM_BITS = 512;
static constexpr idx_t LOG_SECTOR_SIZE = 6;             // a sector is 64 bits, log2(64) = 6
static constexpr idx_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F; // 6 bits for 64 positions
static constexpr idx_t N_BITS = 4;                      // the number of bits to set per hash

void BloomFilter::Initialize(ClientContext &context_p, idx_t number_of_rows) {
	BufferManager &buffer_manager = BufferManager::GetBufferManager(context_p);

	const idx_t min_bits = std::max<idx_t>(MIN_NUM_BITS, number_of_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(NextPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	bitmask = num_sectors - 1;

	buf_ = buffer_manager.GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint64_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	bf = reinterpret_cast<uint64_t *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	std::fill_n(bf, num_sectors, 0);

	initialized = true;
}

inline uint64_t GetMask(const hash_t hash) {
	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);

	uint64_t mask = 0;

	for (idx_t bit_idx = 8 - N_BITS; bit_idx < 8; bit_idx++) {
		const uint8_t bit_pos = shifts_8[bit_idx];
		mask |= (1ULL << bit_pos);
	}

	return mask;
}

void BloomFilter::InsertHashes(const Vector &hashes_v, idx_t count) const {
	auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	for (idx_t i = 0; i < count; i++) {
		InsertOne(hashes[i]);
	}
}

idx_t BloomFilter::LookupHashes(const Vector &hashes_v, SelectionVector &result_sel, const idx_t count) const {
	D_ASSERT(hashes_v.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(hashes_v.GetType() == LogicalType::HASH);

	const auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		result_sel.set_index(found_count, i);
		found_count += LookupOne(hashes[i]);
	}
	return found_count;
}

inline void BloomFilter::InsertOne(const hash_t hash) const {
	D_ASSERT(initialized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(hash);
	std::atomic<uint64_t> &slot = *reinterpret_cast<std::atomic<uint64_t> *>(&bf[bf_offset]);

	slot.fetch_or(mask, std::memory_order_relaxed);
}

inline bool BloomFilter::LookupOne(const uint64_t hash) const {
	D_ASSERT(initialized);
	const uint64_t bf_offset = hash & bitmask;
	const uint64_t mask = GetMask(hash);

	return (bf[bf_offset] & mask) == mask;
}

string BFTableFilter::ToString(const string &column_name) const {
	return column_name + " IN BF(" + key_column_name + ")";
}

void BFTableFilter::HashInternal(Vector &keys_v, const SelectionVector &sel, idx_t approved_count, Vector &hashes_v) {
	if (sel.IsSet()) {
		Vector keys_sliced_v(keys_v.GetType(), approved_count);
		VectorOperations::Copy(keys_v, keys_sliced_v, sel, approved_count,0 , 0);
		VectorOperations::Hash(keys_sliced_v, hashes_v, approved_count);
	} else {
		VectorOperations::Hash(keys_v, hashes_v, approved_count);
	}
}


idx_t BFTableFilter::Filter(Vector &keys_v, SelectionVector &input_sel, idx_t &approved_tuple_count,
                            BFTableFilterState &_state) const {

	// copy the sel vector
	SelectionVector copy_sel(approved_tuple_count);
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		copy_sel.set_index(i, input_sel.get_index(i));
	}

	Vector hashes_v(LogicalType::HASH, approved_tuple_count);
	SelectionVector bf_sel(approved_tuple_count);


	HashInternal(keys_v, copy_sel, approved_tuple_count, hashes_v);

	idx_t found_count;
	if (hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR && false) {
		const auto constant_hash = *ConstantVector::GetData<hash_t>(hashes_v);
		const bool found = this->filter.LookupOne(constant_hash);
		found_count = found ? approved_tuple_count : 0;
	} else {
		hashes_v.Flatten(approved_tuple_count);
		found_count = this->filter.LookupHashes(hashes_v, bf_sel, approved_tuple_count);
	}

	// all the elements have been found, we don't need to translate anything
	if (found_count == approved_tuple_count) {
		return approved_tuple_count;
	}

	if (!input_sel.IsSet()) {
		input_sel.Initialize(approved_tuple_count);
	}

	for (idx_t idx = 0; idx < found_count; idx++) {
		const idx_t flat_sel_idx = bf_sel.get_index(idx);
		const idx_t original_sel_idx = copy_sel.get_index(flat_sel_idx);
		input_sel.set_index(idx, original_sel_idx);
	}

	// printf("BFTableFilter::Filter: column: %s, approved_tuple_count: %llu, found_count: %llu, sel_is_set: %d\n",
	//        key_column_name.c_str(),
	//        approved_tuple_count, found_count, input_sel.IsSet());
	approved_tuple_count = found_count;
	return approved_tuple_count;
}

bool BFTableFilter::FilterValue(const Value &value) const {
	const auto hash = value.Hash();
	return filter.LookupOne(hash);
}

FilterPropagateResult BFTableFilter::CheckStatistics(BaseStatistics &stats) const {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

bool BFTableFilter::Equals(const TableFilter &other) const {
	if (!TableFilter::Equals(other)) {
		return false;
	}
	return false;
}
unique_ptr<TableFilter> BFTableFilter::Copy() const {
	return make_uniq<BFTableFilter>(this->filter, this->filters_null_values, this->key_column_name, this->key_type);
}

unique_ptr<Expression> BFTableFilter::ToExpression(const Expression &column) const {
	auto bound_constant = make_uniq<BoundConstantExpression>(Value(true));
	return std::move(bound_constant);
}

void BFTableFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
	serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
	serializer.WriteProperty<LogicalType>(202, "key_type", key_type);
}

unique_ptr<TableFilter> BFTableFilter::Deserialize(Deserializer &deserializer) {
	auto filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
	auto key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
	auto key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");

	BloomFilter filter;
	auto result = make_uniq<BFTableFilter>(filter, filters_null_values, key_column_name, key_type);
	return std::move(result);
}

} // namespace duckdb
