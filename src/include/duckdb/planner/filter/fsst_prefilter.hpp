//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/fsst_prefilter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/variable_binary_buffer.hpp"

namespace duckdb {

//! The substring prefilter of the fsst repository (third_party/fsst/upstream/search), driven over an
//! FSST vector. Planning turns each needle into a probe cover over the segment's symbol table; the scan
//! walks the compressed code stream and returns the rows holding a covered code. Those rows are a
//! superset of the matches - the exact predicate downstream is the correctness authority.
class FSSTPrefilter {
public:
	FSSTPrefilter();
	~FSSTPrefilter();

	//! Make sure a cover for `needles` is planned over the vector's symbol table, weighted by the code
	//! counts the encoder stored with it. Planning happens once per row group: every segment of a row
	//! group shares the table, so a vector with the planned row_group_id reuses the plan unchecked.
	//! The counts are per segment, so a plan from the first segment can be poor for the rest; that is
	//! accepted. row_group_id 0 means unknown and plans every time. Returns whether a usable cover exists.
	bool Prepare(idx_t row_group_id, const void *decoder, idx_t symbol_count, const vector<string> &needles,
	             idx_t row_count);

	//! Scan the vector's code stream and keep the rows of `sel` that hold a covered code. False when the
	//! vector's layout is not one contiguous descending block, in which case nothing is written.
	bool Filter(const var_binary_view_t &view, idx_t row_count, const SelectionVector &sel, idx_t count,
	            SelectionVector &result_sel, idx_t &result_count);

private:
	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
