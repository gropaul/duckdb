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

//! A LIKE pattern as the literal runs between its wildcards, plus where it is anchored. `contains(col, x)`
//! is one run with no anchor, `prefix(col, x)` one run anchored at the start, `suffix(col, x)` one anchored
//! at the end, `x%y%` two runs anchored at the start. One run anchored at most at one end is answered
//! exactly; anything else is a superset and the exact predicate has to run behind it.
struct FSSTPattern {
	vector<string> runs;
	bool anchor_start;
	bool anchor_end;
	//! NOT LIKE / NOT contains(...): keep the valid rows that do not match
	bool negated = false;

	bool IsExact() const {
		return runs.size() == 1 && !(anchor_start && anchor_end);
	}
};

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
	bool Prepare(idx_t row_group_id, const void *decoder, idx_t symbol_count, const vector<FSSTPattern> &patterns,
	             idx_t row_count);

	//! Whether the last Prepare planned rows that are the answer rather than a superset. False means the
	//! caller must still run the exact predicate over the rows Filter keeps.
	bool PlanIsExact() const;

	//! Keep the rows of `sel` that are valid and contain one of the needles, or for a negated pattern the
	//! valid rows that do not. False when a plan that scans the code stream gets a vector that is not one
	//! contiguous descending block, or a negated pattern has only a superset plan, in which case nothing is
	//! written; an anchored plan reads each row on its own and takes any layout.
	bool Filter(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
	            const SelectionVector &sel, idx_t count, SelectionVector &result_sel, idx_t &result_count);

private:
	//! The rows of `sel` that are valid and match, ascending, into result_sel
	bool FilterMatches(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
	                   const SelectionVector &sel, idx_t count, SelectionVector &result_sel, idx_t &result_count);

	struct State;
	unique_ptr<State> state;
};

} // namespace duckdb
