

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

		// todo: we don't need to check the next pointer as we know the chain length
		for (idx_t idx = 0; idx < chains_remaining; idx++) {

			auto sel_idx = probe_state.chains_remaining_sel.get_index(idx);
			auto &fact_data = fact_data_res[sel_idx];

			data_ptr_t &tail = pointers[sel_idx];
			data_ptr_t next_pointer = Load<data_ptr_t>(tail + next_pointer_offset);

			if (next_pointer != nullptr) {

				fact_data->pointers[chain_element_index] = next_pointer;
				pointers[sel_idx] = next_pointer;

				probe_state.chains_remaining_sel.set_index(next_chains_remaining++, sel_idx);
			} else {
				fact_data->SortKeys();
			}
		}

		chains_remaining = next_chains_remaining;
	}
}

// Function to determine which side of a join operation should be used for building and probing
static void DetermineBuildAndProbeSides(fact_data_t &left, fact_data_t &right, fact_data_t *&build_side,
                                        fact_data_t *&probe_side) {
	bool lhs_built = left.KeyMapBuilt();
	bool rhs_built = right.KeyMapBuilt();

	// Decide on the build and probe side based on whether the key maps have been built
	if (lhs_built && !rhs_built) {
		build_side = &left;
		probe_side = &right;
	} else if (!lhs_built && rhs_built) {
		build_side = &right;
		probe_side = &left;
	} else {
		// If both or neither are built, choose the smaller one as the build side to optimize performance
		if (left.chain_length < right.chain_length) {
			build_side = &left;
			probe_side = &right;
		} else {
			build_side = &right;
			probe_side = &left;
		}
	}
}

// We always have to return the rhs pointers to make sure that we can expand on the rhs
static void Intersect(fact_data_t &left, const fact_data_t &right, data_ptr_t *&lhs_pointers_res,
                      data_ptr_t *&rhs_pointers_res, idx_t &pointers_index) {

	pointers_index = 0;

	// intersect to sorted lists
	idx_t left_index = 0;
	idx_t right_index = 0;

	while (left_index < left.chain_length && right_index < right.chain_length) {
		uint64_t &left_key = left.keys[left_index];
		uint64_t &right_key = right.keys[right_index];

		if (left_key == right_key) {
			lhs_pointers_res[pointers_index] = left.pointers[left_index];
			rhs_pointers_res[pointers_index] = right.pointers[right_index];
			pointers_index += 1;
			left_index += 1;
			right_index += 1;
		} else {
			bool left_key_smaller = (left_key < right_key);
			left_index += left_key_smaller;
			right_index += !left_key_smaller;
		}
	}

}
} // namespace duckdb