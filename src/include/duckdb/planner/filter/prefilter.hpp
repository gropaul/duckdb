//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/prefilter.hpp
//
// Batch substring prefilter, single- and multi-needle. Scans a vector of rows
// for fixed-width byte codes and writes the indices of rows containing ANY of
// the codes into a selection vector. Two widths share one algorithm:
//
//     2-byte codes (uint16_t)  e.g. an FSST mandatory chain of 2 codes
//     4-byte codes (uint32_t)  e.g. a needle prefix or a 4-code chain
//
// Properties (the conservative set - the kernel assumes nothing):
//   * EXACT: result equals std::string_view::find != npos, OR-ed over codes.
//   * Safe on a raw, unfiltered column: null rows and rows shorter than the
//     code width are rejected by a single guard.
//   * No out-of-bounds reads: every load stays inside the row. No trailing
//     padding or buffer-slack contract is required.
//   * Portable C++ (no intrinsics), no allocations, stateless / thread-safe.
//
// The kernels are tuned and benchmarked for up to 8 codes; beyond that a
// shared-filter algorithm (Teddy-style) wins. Duplicate codes are harmless.
// Matching is raw byte comparison (no case folding, NUL bytes are fine).
// The implementations live in prefilter.cpp on purpose - see the codegen
// warning there.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector/variable_binary_buffer.hpp"

namespace duckdb {

// Scans the first `count` rows of `sel` (row i is `strings[sel[i]]`), skipping null and
// too-short rows, and writes rows that contain ANY target into `result_sel` (ascending,
// room for `count` entries). Returns the match count.
idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count);
idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count);

// Same driver over the FSST vector layout: row i is view.GetVarBinary(i)
// (positional base + offsets + lengths, see FSSTVector::GetDataView).
idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count);
idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count);

} // namespace duckdb
