

#include "duckdb/execution/fact_utils.hpp"

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

static idx_t GetNextPointerOffset(const TupleDataCollection *data_collection) {
	return data_collection->GetLayout().GetOffsets().back();
}

static void GetChainData(Vector &pointers_v, TupleDataCollection *data_collection, column_t key_column,
                         const SelectionVector &sel, idx_t count, fact_data_t *(&fact_data_res)[STANDARD_VECTOR_SIZE],
                         JoinHashTable::FactProbeState &probe_state) {

	D_ASSERT(pointers_v.GetType().id() == LogicalTypeId::FACT_POINTER ||
	         pointers_v.GetType().id() == LogicalTypeId::POINTER);

	// this can happen if e.g. the pointers come from the lhs
	bool flatten_needed = pointers_v.GetVectorType() != VectorType::FLAT_VECTOR;
	if (flatten_needed) {
		pointers_v.Flatten(count);
		pointers_v.Resize(count, STANDARD_VECTOR_SIZE);
	}

	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_v);

	for (idx_t idx = 0; idx < count; idx++) {
		idx_t sel_idx = sel.get_index(idx);
		data_ptr_t ptr = flatten_needed ? pointers[idx] : pointers[sel_idx];
		fact_data_res[sel_idx] = reinterpret_cast<fact_data_t *>(ptr);
	}

	auto tmp_data = FlatVector::GetData<uint64_t>(probe_state.tmp_data_v);

	idx_t chains_remaining = 0;
	// insert the head into the data, set the selection vector to point to all heads
	for (idx_t idx = 0; idx < count; idx++) {
		idx_t sel_idx = sel.get_index(idx);
		// the get row pointers has this informal dictionary vector consisting of a flat and a
		// sel vector

		// only process  the chain data if it has not been gathered yet
		if (fact_data_res[sel_idx]->keys_gathered) {
			continue;
		} else {

			fact_data_res[sel_idx]->keys_gathered = true;
			pointers[sel_idx] = fact_data_res[sel_idx]->chain_head;

			auto &data_ptrs = fact_data_res[sel_idx]->pointers;
			data_ptrs[0] = pointers[sel_idx];

			probe_state.chains_remaining_sel.set_index(chains_remaining, sel_idx);
			chains_remaining += 1;
		}
	}

	idx_t next_pointer_offset = GetNextPointerOffset(data_collection);

	idx_t chain_element_index = 0;
	// Advance the pointers until we reach the end of the chain
	while (chains_remaining != 0) {

		// load the key column for the pointers we have

		data_collection->Gather(pointers_v, probe_state.chains_remaining_sel, chains_remaining, key_column,
		                        probe_state.tmp_data_v, probe_state.chains_remaining_sel, nullptr);

		for (idx_t idx = 0; idx < chains_remaining; idx++) {
			auto sel_idx = probe_state.chains_remaining_sel.get_index(idx);
			auto &fact_data = fact_data_res[sel_idx];
			auto key = tmp_data[sel_idx];

			fact_data->keys[chain_element_index] = key;
		}

		// advance the pointers
		idx_t next_chains_remaining = 0;
		chain_element_index += 1;

		for (idx_t idx = 0; idx < chains_remaining; idx++) {

			auto sel_idx = probe_state.chains_remaining_sel.get_index(idx);
			auto &fact_data = fact_data_res[sel_idx];

			if (chain_element_index < fact_data->chain_length) {

				data_ptr_t &tail = pointers[sel_idx];
				data_ptr_t next_pointer = Load<data_ptr_t>(tail + next_pointer_offset);

				fact_data->pointers[chain_element_index] = next_pointer;
				pointers[sel_idx] = next_pointer;
				probe_state.chains_remaining_sel.set_index(next_chains_remaining++, sel_idx);
			}
		}

		chains_remaining = next_chains_remaining;
	}
}

// Function to determine which side of a join operation should be used for building and probing
// 1. if one of the sides is already built, we should use the other side for building and probing
// 2. if both sides are built or none of them is built, we should use the smaller side for building and probing
static void DetermineSidesAndBuild(fact_data_t *&build_side, fact_data_t *&probe_side, data_ptr_t *&build_res,
                                        data_ptr_t *&probe_res) {

	bool current_build_side_build = build_side->IsHTBuild();
	bool current_probe_side_build = probe_side->IsHTBuild();

	// Determine which side to build and which to probe
	if (current_build_side_build && !current_probe_side_build) {
		// Current build side is built, use the other side for probing, everything is already set up
	} else if (!current_build_side_build && current_probe_side_build) {
		// Current build side is not built, but the probe side is built, swap the sides
		std::swap(build_side, probe_side);
		std::swap(build_res, probe_res);
	} else {
		// Either both sides are built or neither side is built
		// Build the smaller side and probe the larger side
		idx_t current_build_side_length = build_side->chain_length;
		idx_t current_probe_side_length = probe_side->chain_length;

		// If the build side is smaller than the probe side, swap the sides
		if (current_build_side_length < current_probe_side_length) {
			std::swap(build_side, probe_side);
			std::swap(build_res, probe_res);
		}

		// if not both sides are built, build the build side
		if (!current_build_side_build) {
			// build the build side
			build_side->BuildHT();
		}
	}

	D_ASSERT(build_side->IsHTBuild());
}

// We always have to return the rhs pointers to make sure that we can expand on the rhs
static void Intersect(fact_data_t *left_ptr, fact_data_t *right_ptr, data_ptr_t *lhs_pointers_res,
                      data_ptr_t *rhs_pointers_res, idx_t &intersection_count) {

	// build on the lhs to probe with the rhs
	DetermineSidesAndBuild(left_ptr, right_ptr, lhs_pointers_res, rhs_pointers_res);

	auto left = *left_ptr;
	auto right = *right_ptr;

	auto &ht = left.chain_ht;
	auto &bitmask = left.ht_bitmask;

	intersection_count = 0;

	// probe the lhs with the rhs
	for (idx_t rhs_idx = 0; rhs_idx < right.chain_length; rhs_idx++) {
		auto rhs_key = right.keys[rhs_idx];
		auto rhs_hash = FactHash(rhs_key, bitmask);
		while (ht[rhs_hash] != NULL_ELEMENT) {
			auto &lhs_idx = ht[rhs_hash];

			if (left.keys[lhs_idx] == rhs_key) {
				lhs_pointers_res[intersection_count] = left.pointers[lhs_idx];
				rhs_pointers_res[intersection_count] = right.pointers[rhs_idx];
				intersection_count += 1;
			}
			IncrementHash(rhs_hash, bitmask);
		}
	}
}
} // namespace duckdb