

#include "duckdb/execution/fact_utils.hpp"

#include "duckdb/common/extra_type_info.hpp"

namespace duckdb {

static idx_t GetNextPointerOffset(const TupleDataCollection *data_collection) {
	return data_collection->GetLayout().GetOffsets().back();
}

static unique_ptr<ChainData[]> GetChainData(Vector &pointers_v, TupleDataCollection *data_collection,
                                            column_t key_column, const SelectionVector &sel, idx_t count) {

	D_ASSERT(pointers_v.GetType().id() == LogicalTypeId::FACT_POINTER ||
	         pointers_v.GetType().id() == LogicalTypeId::POINTER);

	unique_ptr<ChainData[]> chains_data(new ChainData[STANDARD_VECTOR_SIZE]);

	// this can happen if e.g. the pointers come from the lhs
	bool flatten_needed = pointers_v.GetVectorType() != VectorType::FLAT_VECTOR;
	if (flatten_needed) {
		pointers_v.Flatten(count);
		pointers_v.Resize(count, STANDARD_VECTOR_SIZE);
	}

	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_v);

	SelectionVector chains_remaining_sel(STANDARD_VECTOR_SIZE);
	Vector tmp_data_v(LogicalType::BIGINT);

	auto tmp_data = FlatVector::GetData<uint64_t>(tmp_data_v);

	// insert the head into the data, set the selection vector to point to all heads
	for (idx_t idx = 0; idx < count; idx++) {
		idx_t sel_idx = sel.get_index(idx);
		// the get row pointers has this informal dictionary vector consisting of an flat and an
		// sel vector
		if (flatten_needed) {
			pointers[sel_idx] = pointers[idx];
		}
		data_ptr_t ptr = pointers[sel_idx];

		chains_data[sel_idx].pointers.push_back(ptr);
		chains_remaining_sel.set_index(idx, sel_idx);
	}

	idx_t chains_remaining = count;

	idx_t next_pointer_offset = GetNextPointerOffset(data_collection);

	// Advance the pointers until we reach the end of the chain
	while (chains_remaining != 0) {

		// load the key column for the pointers we have

		data_collection->Gather(pointers_v, chains_remaining_sel, chains_remaining, key_column, tmp_data_v,
		                        chains_remaining_sel, nullptr);

		for (idx_t idx = 0; idx < chains_remaining; idx++) {
			auto sel_idx = chains_remaining_sel.get_index(idx);
			auto &data = chains_data[sel_idx];
			auto key = tmp_data[sel_idx];

			data.keys.push_back(key);
		}

		idx_t next_chains_remaining = 0;

		// advance the pointers

		for (idx_t idx = 0; idx < chains_remaining; idx++) {

			auto sel_idx = chains_remaining_sel.get_index(idx);
			auto &data = chains_data[sel_idx];

			data_ptr_t &tail = pointers[sel_idx];
			data_ptr_t next_pointer = Load<data_ptr_t>(tail + next_pointer_offset);

			if (next_pointer != nullptr) {

				data.pointers.push_back(next_pointer);
				pointers[sel_idx] = next_pointer;

				chains_remaining_sel.set_index(next_chains_remaining++, sel_idx);
			}
		}

		chains_remaining = next_chains_remaining;
	}

	return chains_data;
}

// Function to determine which side of a join operation should be used for building and probing
static void DetermineBuildAndProbeSides(ChainData &left, ChainData &right, ChainData *&build_side,
                                        ChainData *&probe_side) {
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
		if (left.keys.size() < right.keys.size()) {
			build_side = &left;
			probe_side = &right;
		} else {
			build_side = &right;
			probe_side = &left;
		}
	}
}

// We always have to return the rhs pointers to make sure that we can expand on the rhs
static void Intersect(ChainData &left, const ChainData &right, vector<data_ptr_t> &lhs_pointers_res,
                      vector<data_ptr_t> &rhs_pointers_res) {
	// build on the lhs to probe with the rhs
	left.BuildKeyMap();

	// probe the lhs with the rhs to return the rhs pointers that are in the lhs
	left.Probe(right, lhs_pointers_res, rhs_pointers_res);

	// the pointers are already expanded in there combination, therefore they need to have
	// the same size
	D_ASSERT(lhs_pointers_res.size() == rhs_pointers_res.size());
}
} // namespace duckdb