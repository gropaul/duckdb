//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/fsst_ops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! Comparison operations evaluated directly on FSST-compressed data, without decompressing.
struct FSSTOps {
	//! Try to evaluate an equality select between an FSST vector and a non-NULL constant VARCHAR by
	//! compressing the constant and comparing in the compressed domain.
	//! Returns false if the input shape does not match; result is only set when returning true.
	DUCKDB_API static bool TryEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                                 idx_t count, optional_ptr<SelectionVector> true_sel,
	                                 optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
	                                 idx_t &result);
	//! TryEquals for not-equals.
	DUCKDB_API static bool TryNotEquals(const Vector &left, const Vector &right,
	                                    optional_ptr<const SelectionVector> sel, idx_t count,
	                                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                                    optional_ptr<ValidityMask> null_mask, idx_t &result);
};

} // namespace duckdb
