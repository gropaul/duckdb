//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/fsst_contains.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class ClientContext;

//! A LIKE pattern as the literals between its wildcards and its anchors: contains is one literal, prefix one
//! anchored at the start, suffix one anchored at the end.
struct FSSTPattern {
	vector<string> literals;
	bool anchor_start;
	bool anchor_end;
	//! NOT LIKE / NOT contains(...)
	bool negated = false;

	//! One literal anchored at most at one end, which the compressed filter answers exactly
	bool IsExact() const {
		return literals.size() == 1 && !(anchor_start && anchor_end);
	}
};

//! A filter for an exact pattern that answers FSST vectors on their codes, with the substring search of
//! third_party/fsst/upstream/search. Other vectors, and those its planner declines, go to `exact`; without one
//! it keeps them, as a prefilter in front of a pattern checked after it. A build without the search returns
//! `exact` itself.
unique_ptr<ExpressionFilterExecutor> CreateContainsExecutor(ClientContext &context, FSSTPattern pattern,
                                                            unique_ptr<ExpressionFilterExecutor> exact);

} // namespace duckdb
