

#include "duckdb/execution/fact_utils.hpp"

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

static idx_t GetNextPointerOffset(const TupleDataCollection &data_collection) {
	return data_collection.GetLayout().GetOffsets().back();
}

static void LoadKeysIfNecessary(UnifiedVectorFormat &offsets_uvf, JoinHashTable *ht, column_t key_column, const SelectionVector &sel,
                                idx_t count, JoinHashTable::FactProbeState &probe_state) {

	auto offsets = UnifiedVectorFormat::GetData<idx_t>(offsets_uvf);
	auto tmp_key_data = FlatVector::GetData<uint64_t>(probe_state.tmp_key_data_v);

	idx_t remaining_chains_to_load = 0;
	idx_t remaining_chains_to_wait_for = 0;
	auto atomic_fact_data_states = reinterpret_cast<atomic<FactDataState> *>(ht->fact_data_states);

	// Set chains_to_load_sel for keys needing loading
	for (idx_t idx = 0; idx < count; idx++) {
		idx_t sel_idx = sel.get_index(idx);
		idx_t uvf_idx = offsets_uvf.sel->get_index(sel_idx);
		idx_t fact_data_offset = offsets[uvf_idx];

		// we will now load the keys for the chains that need loading, so set the state to KEYS_LOADING_IN_PROGRESS
		// with a CAS operation
		FactDataState expected = FactDataState::KEYS_NOT_LOADED;
		FactDataState desired = FactDataState::KEYS_LOADING_IN_PROGRESS;
		bool needs_loading = atomic_fact_data_states[fact_data_offset].compare_exchange_weak(expected, desired);

		probe_state.chains_to_load_sel.set_index(remaining_chains_to_load, sel_idx);
		remaining_chains_to_load += needs_loading;

		// if the state was already KEYS_LOADING_IN_PROGRESS, we need to wait for the keys to be loaded
		bool is_loading = atomic_fact_data_states[fact_data_offset].load(std::memory_order_relaxed) == FactDataState::KEYS_LOADING_IN_PROGRESS;
		probe_state.chains_to_wait_for_sel.set_index(remaining_chains_to_wait_for, sel_idx);
		remaining_chains_to_wait_for += is_loading;

	}

	const TupleDataCollection &data_collection = ht->GetDataCollection();
	const idx_t next_pointer_offset = GetNextPointerOffset(data_collection);

	// initialize the pointers for the chains that need loading
	auto tmp_pointers = FlatVector::GetData<data_ptr_t>(probe_state.tmp_pointer_v);
	for (idx_t idx = 0; idx < remaining_chains_to_load; idx++) {
		auto sel_idx = probe_state.chains_to_load_sel.get_index(idx);
		auto uvf_idx = offsets_uvf.sel->get_index(sel_idx);
		auto fact_offset = offsets[uvf_idx];
		auto fact_data = ht->fact_datas[fact_offset];
		auto chain_head = fact_data.chain_head;
		tmp_pointers[sel_idx] = chain_head;
	}

	// we will now load for each fact_data first the 0th element, then the 1st element, etc. chain_element_index keeps
	// track of the current element
	idx_t chain_element_index = 0;

	// Advance the pointers until we reach the end of the chain
	while (remaining_chains_to_load != 0) {

		// load the key column for the pointers we have
		data_collection.Gather(probe_state.tmp_pointer_v, probe_state.chains_to_load_sel, remaining_chains_to_load,
		                       key_column, probe_state.tmp_key_data_v, probe_state.chains_to_load_sel, nullptr);

		for (idx_t idx = 0; idx < remaining_chains_to_load; idx++) {
			auto sel_idx = probe_state.chains_to_load_sel.get_index(idx);
			auto uvf_idx = offsets_uvf.sel->get_index(sel_idx);
			auto fact_offset = offsets[uvf_idx];
			auto fact_data = ht->fact_datas[fact_offset];

			auto key = tmp_key_data[sel_idx];

			// update the key and the pointer to that key
			fact_data.keys[chain_element_index] = key;
			fact_data.pointers[chain_element_index] = tmp_pointers[sel_idx];
		}

		// advance the pointers, keep all elements in the selection vector that still have chains to load
		chain_element_index += 1;
		idx_t next_chains_remaining = 0;

		for (idx_t idx = 0; idx < remaining_chains_to_load; idx++) {

			auto sel_idx = probe_state.chains_to_load_sel.get_index(idx);
			auto uvf_idx = offsets_uvf.sel->get_index(sel_idx);
			auto fact_offset = offsets[uvf_idx];
			auto fact_data = ht->fact_datas[fact_offset];

			if (chain_element_index < fact_data.chain_length) {

				// update the pointer to the next pointer
				data_ptr_t &tail = tmp_pointers[sel_idx];
				data_ptr_t next_pointer = Load<data_ptr_t>(tail + next_pointer_offset);
				tmp_pointers[sel_idx] = next_pointer;

				probe_state.chains_to_load_sel.set_index(next_chains_remaining, sel_idx);
				next_chains_remaining += 1;
			} else {
				// if we have reached the end of the chain, set the state to KEYS_LOADED
				atomic_fact_data_states[fact_offset].store(FactDataState::KEYS_LOADED, std::memory_order_relaxed);
			}
		}

		remaining_chains_to_load = next_chains_remaining;
	}

	// wait for the keys to be loaded
	for (idx_t idx = 0; idx < remaining_chains_to_wait_for; idx++) {
		idx_t sel_idx = probe_state.chains_to_wait_for_sel.get_index(idx);
		idx_t fact_data_offset = offsets[sel_idx];
		while (atomic_fact_data_states[fact_data_offset].load(std::memory_order_relaxed) == FactDataState::KEYS_LOADING_IN_PROGRESS) {
		}
	}
}

static void BuildFactHTIfNecessary(fact_data_t &fact_data, atomic<FactDataState> &state) {
	// use a compare and swap operation to set the state to HT_BUILD_IN_PROGRESS
	FactDataState expected = FactDataState::KEYS_LOADED;
	FactDataState desired = FactDataState::HT_BUILD_IN_PROGRESS;

	bool needs_building = state.compare_exchange_weak(expected, desired);

	if (needs_building) {
		fact_data.BuildHT();
		state.store(FactDataState::HT_BUILT_COMPLETED, std::memory_order_relaxed);
	} else {
		// spin lock until the state is HT_BUILT_COMPLETED
		while (state.load(std::memory_order_relaxed) != FactDataState::HT_BUILT_COMPLETED) {
		}
	}
}

// Function to determine which side of a join operation should be used for building and probing, returns true of the
// sides should be swapped. RHS is the original build side
static bool DetermineSidesAndBuildHT(const JoinHashTable *lhs_ht, const JoinHashTable *rhs_ht,
                                     const idx_t lhs_fact_data_offset, const idx_t rhs_fact_data_offset,
                                     fact_data_t lhs_fact_data, fact_data_t rhs_fact_data) {

	auto atomic_lhs_states = reinterpret_cast<atomic<FactDataState> *>(lhs_ht->fact_data_states);
	auto atomic_rhs_states = reinterpret_cast<atomic<FactDataState> *>(rhs_ht->fact_data_states);

	atomic<FactDataState> &lhs_state = atomic_lhs_states[lhs_fact_data_offset];
	atomic<FactDataState> &rhs_state = atomic_rhs_states[rhs_fact_data_offset];

	bool current_lhs_build = lhs_state.load(std::memory_order_relaxed) == FactDataState::HT_BUILT_COMPLETED;
	bool current_rhs_build = rhs_state.load(std::memory_order_relaxed) == FactDataState::HT_BUILT_COMPLETED;

	if (!current_lhs_build && current_rhs_build) {
		return false; // current lhs is not built, but the rhs is built -> no swap needed
	} else if (current_lhs_build && !current_rhs_build) {
		return true; // Current lhs is built, but the rhs is not built -> swap needed
	}
	// Either both sides are built or neither side is built -> build on the smaller side
	else {
		if (lhs_fact_data.chain_length > rhs_fact_data.chain_length) {
			BuildFactHTIfNecessary(lhs_fact_data, lhs_state);
			return true; // Build on the lhs side -> swap needed
		} else {
			BuildFactHTIfNecessary(rhs_fact_data, rhs_state);
			return false; // Build on the rhs side -> no swap needed
		}
	}
}

static void IntersectFacts(const fact_data_t &lhs_fact_data, const fact_data_t &rhs_fact_data, data_ptr_t *lhs_pointers_res,
						   data_ptr_t *rhs_pointers_res, idx_t &intersection_count) {
	auto &ht = rhs_fact_data.chain_ht;
	auto &bitmask = rhs_fact_data.ht_bitmask;

	intersection_count = 0;

	// probe the lhs with the rhs
	for (idx_t lhs_idx = 0; lhs_idx < lhs_fact_data.chain_length; lhs_idx++) {
		auto lhs_key = lhs_fact_data.keys[lhs_idx];
		auto lhs_offset = FactHash(lhs_key, bitmask);
		while (ht[lhs_offset] != NULL_ELEMENT) {
			idx_t &rhs_idx = ht[lhs_offset];
			auto rhs_key = rhs_fact_data.keys[rhs_idx];
			if (lhs_key == rhs_key) {
				lhs_pointers_res[intersection_count] = lhs_fact_data.pointers[lhs_idx];
				rhs_pointers_res[intersection_count] = rhs_fact_data.pointers[rhs_idx];
				intersection_count += 1;
			}
			IncrementHash(lhs_offset, bitmask);
		}
	}
}

// We always have to return the rhs pointers to make sure that we can expand on the rhs
static void BuildAndIntersectFacts(const JoinHashTable *lhs_ht, const JoinHashTable *rhs_ht, const idx_t lhs_fact_data_offset,
                      const idx_t rhs_fact_data_offset, data_ptr_t *lhs_pointers_res, data_ptr_t *rhs_pointers_res,
                      idx_t &intersection_count) {

	fact_data_t &lhs_fact_data = lhs_ht->fact_datas[lhs_fact_data_offset];
	fact_data_t &rhs_fact_data = rhs_ht->fact_datas[rhs_fact_data_offset];

	// build on the lhs to probe with the rhs
	bool swap_sides = DetermineSidesAndBuildHT(lhs_ht, rhs_ht, lhs_fact_data_offset, rhs_fact_data_offset,
	                                           lhs_fact_data, rhs_fact_data);

	if (swap_sides) {
		IntersectFacts(rhs_fact_data, lhs_fact_data, rhs_pointers_res, lhs_pointers_res, intersection_count);
	} else {
		IntersectFacts(lhs_fact_data, rhs_fact_data, lhs_pointers_res, rhs_pointers_res, intersection_count);
	}
}


} // namespace duckdb