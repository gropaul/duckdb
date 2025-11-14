//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/early_probing
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

static bool IsHashContained(const JoinHashTable &ht, const hash_t hash) {

	bool has_match = false;
	bool has_empty = false;

	const hash_t row_salt = ht_entry_t::ExtractSalt(hash);

	constexpr idx_t PROBING_LENGTH = 4;

	for (idx_t j = 0; j < PROBING_LENGTH; j++) {
		const idx_t ht_offset = (hash + j) & ht.bitmask;
		const ht_entry_t entry = ht.GetPointerTable()[ht_offset];
		const bool occupied = entry.IsOccupied();
		const bool salt_match = entry.GetSalt() == row_salt;
		if (!occupied) {
			return false;
		}

		if (salt_match) {
			return true;
		}
	}

	return true;
	// -> we keep it
	// a) if there is a salt match -> we found our slot
	// b) or if there is no empty entry -> we would need to continue probing
	const bool keep_tuple = has_match || (!has_empty);
	return keep_tuple;
}

static __attribute__((noinline)) idx_t EarlyProbeHashes(Vector &hashes_dense_v, JoinHashTable &ht, SelectionVector &sel, const idx_t count) {

	idx_t found_count = 0;
	const auto hashes_dense = FlatVector::GetData<hash_t>(hashes_dense_v);

	for (idx_t i = 0; i < count; i++) {

		const hash_t row_hash = hashes_dense[i];
		const bool found = IsHashContained(ht, row_hash);
		sel.set_index(found_count, i);
		found_count += found;
	}

	return found_count;
}

class EarlyProbingFilter : public TableFilter {

private:
	JoinHashTable &hashtable;
	bool filters_null_values;
	string key_column_name;
	LogicalType key_type;

public:
	static constexpr auto TYPE = TableFilterType::EARLY_PROBING;

public:
	explicit EarlyProbingFilter(JoinHashTable &hashtable_p, const bool filters_null_values_p,
	                            const string &key_column_name_p, const LogicalType &key_type_p)
	    : TableFilter(TYPE), hashtable(hashtable_p), filters_null_values(filters_null_values_p),
	      key_column_name(key_column_name_p), key_type(key_type_p) {
	}

	//! If the join condition is e.g. "A = B", the bf will filter null values.
	//! If the condition is "A is B" the filter will let nulls pass
	bool FiltersNullValues() const {
		return filters_null_values;
	}

	LogicalType GetKeyType() const {
		return key_type;
	}

public:
	string ToString(const string &column_name) const override;

	static void HashInternal(Vector &keys_v, const SelectionVector &sel, idx_t &approved_count,
	                         EarlyProbingFilterState &state) {
		if (sel.IsSet()) {
			VectorOperations::Copy(keys_v, state.keys_flat_v, sel, approved_count, 0, 0);
			D_ASSERT(state.keys_flat_v.GetVectorType() == VectorType::FLAT_VECTOR);
			VectorOperations::Hash(state.keys_flat_v, state.hashes_v,
			                       approved_count); // todo: we actually only want to hash the sel!

		} else {
			VectorOperations::Hash(keys_v, state.hashes_v,
			                       approved_count); // todo: we actually only want to hash the sel!
		}
	}

	// Filters the data by first hashing and then probing the bloom filter. The &sel will hold
	// the remaining tuples, &approved_tuple_count will hold the approved count.
	idx_t Filter(Vector &keys_v, UnifiedVectorFormat &keys_uvf, SelectionVector &sel, idx_t &approved_tuple_count,
	             EarlyProbingFilterState &state) const {

		// printf("Filter bf: bf has %llu sectors and initialized=%hd \n", filter.num_sectors, filter.IsInitialized());
		// todo: Don't do this if it is a perfect hash table, instead don't insert the Early Probing at all
		const bool isPerfectHT =
		    hashtable.capacity != hashtable.bitmask + 1; // todo: this is a hacky way to check if the ht is perfect
		if (!state.continue_filtering || isPerfectHT || approved_tuple_count == 0) {
			return approved_tuple_count;
		}

		if (state.current_capacity < approved_tuple_count) {
			state.keys_flat_v.Initialize(false, approved_tuple_count);
			state.hashes_v.Initialize(false, approved_tuple_count);
			state.bf_sel.Initialize(approved_tuple_count);
			state.current_capacity = approved_tuple_count;
		}

		HashInternal(keys_v, sel, approved_tuple_count, state);

		// todo: we need to properly find out how one would densify the hashes here!
		idx_t found_count;
		if (state.hashes_v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			const auto &hash = *ConstantVector::GetData<hash_t>(state.hashes_v);
			const bool found = IsHashContained(hashtable, hash);
			found_count = found ? approved_tuple_count : 0;
		} else {
			state.hashes_v.Flatten(approved_tuple_count);
			found_count = EarlyProbeHashes(state.hashes_v, hashtable, state.bf_sel, approved_tuple_count);
		}

		// add the runtime statistics to stop using the bf if not selective
		if (state.vectors_processed < 10) {
			state.vectors_processed += 1;
			state.tuples_accepted += found_count;
			state.tuples_processed += approved_tuple_count;

			if (state.vectors_processed == 10) {
				const double selectivity =
				    static_cast<double>(state.tuples_accepted) / static_cast<double>(state.tuples_processed);
				printf("BF Sel=%f, HT Size=%lu, HT Capacity=%lu\n", selectivity, hashtable.Count(),
				hashtable.capacity);
				if (selectivity > 0.25) {
					state.continue_filtering = false;
					printf("Stop filtering\n");

				}
			}
		}

		// all the elements have been found, we don't need to translate anything
		if (found_count == approved_tuple_count) {
			return approved_tuple_count;
		}

		if (sel.IsSet()) {
			for (idx_t idx = 0; idx < found_count; idx++) {
				const idx_t flat_sel_idx = state.bf_sel.get_index(idx);
				const idx_t original_sel_idx = sel.get_index(flat_sel_idx);
				sel.set_index(idx, original_sel_idx);
			}
		} else {
			sel.Initialize(state.bf_sel); // maybe we could also reference here?
		}

		approved_tuple_count = found_count;
		return approved_tuple_count;
	}

	bool FilterValue(const Value &value) const {
		const auto hash = value.Hash();
		return IsHashContained(hashtable, hash);
	}

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	bool Equals(const TableFilter &other) const override {
		if (!TableFilter::Equals(other)) {
			return false;
		}

		// todo: not really sure what to do here :(
		return false;
	}
	unique_ptr<TableFilter> Copy() const override {
		return make_uniq<EarlyProbingFilter>(this->hashtable, this->filters_null_values, this->key_column_name,
		                                     this->key_type);
	}

	unique_ptr<Expression> ToExpression(const Expression &column) const override;

	void Serialize(Serializer &serializer) const override {

		TableFilter::Serialize(serializer);
		serializer.WriteProperty<bool>(200, "filters_null_values", filters_null_values);
		serializer.WriteProperty<string>(201, "key_column_name", key_column_name);
		serializer.WriteProperty<LogicalType>(202, "key_type", key_type);

		// todo: How/Should be serialize the bloom filter?
	}
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer) {
		throw NotImplementedException("Deserializing the Early Probe is not possible due to HashTable serialization");
		// bool filters_null_values = deserializer.ReadProperty<bool>(200, "filters_null_values");
		// string key_column_name = deserializer.ReadProperty<string>(201, "key_column_name");
		// LogicalType key_type = deserializer.ReadProperty<LogicalType>(202, "key_type");
		//
		// CacheSectorizedBloomFilter filter;
		// auto result = make_uniq<BloomFilter>(, filters_null_values, key_column_name, key_type);
		// return std::move(result);
	}
};

} // namespace duckdb
