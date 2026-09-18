//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/fsst_contains.hpp
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

//! contains(col, needle) for one or more needles, evaluated on an FSST vector in the compressed domain
//! with the substring search of the fsst repository (third_party/fsst/upstream/search). Planning turns
//! each needle into a probe cover over the segment's symbol table and an alignment walk; the scan finds
//! the rows holding a covered code and the walk verifies every hit, so the rows come out exact.
class FSSTContains {
public:
	FSSTContains();
	~FSSTContains();

	//! Make sure the needles are planned over the vector's symbol table, weighted by the code counts the
	//! encoder stored with it. Planning happens once per row group: every segment of a row group shares
	//! the table, so a vector with the planned row_group_id reuses the plan unchecked. The counts are per
	//! segment, so a plan from the first segment can be poor for the rest; that is accepted. row_group_id
	//! 0 means unknown and plans every time. Returns whether a usable plan exists.
	bool Prepare(idx_t row_group_id, const void *decoder, idx_t symbol_count, const vector<string> &needles,
	             idx_t row_count);

	//! Keep the rows of `sel` that are valid and contain one of the needles. False when the vector's
	//! layout is not one contiguous descending block, in which case nothing is written.
	bool Filter(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
	            const SelectionVector &sel, idx_t count, SelectionVector &result_sel, idx_t &result_count);

private:
	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
