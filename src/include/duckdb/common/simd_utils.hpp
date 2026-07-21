//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/simd_utils.hpp
//
// Portable SWAR / auto-vectorizable byte primitives. The kernels are plain scalar loops written so
// the compiler unrolls and vectorizes them - no architecture intrinsics. Add reusable byte/word
// scan drivers here; the implementations live in simd_utils.cpp.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

//! Width in bytes of a single comparison code (the window the byte-scan kernels match at a time).
constexpr uint32_t CODE_LEN = sizeof(uint32_t);

// Prefilter driver. Scans the first `count` rows of `sel` (row i is `strings[sel[i]]`) for the
// CODE_LEN-byte `pattern`, skipping null and sub-CODE_LEN rows, and writing matches into
// `result_sel`. Returns the match count.
idx_t k_vert_u32(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                 SelectionVector &result_sel, idx_t count, const char *pattern);

// Same driver over the FSST vector layout: row i is base[offsets[i + 1] : offsets[i]]
// (descending physical offsets, see FSSTVector::GetOffsets).
idx_t k_vert_u32(const char *base, const int32_t *offsets, const ValidityMask &validity, const SelectionVector &sel,
                 SelectionVector &result_sel, idx_t count, const char *pattern);

// Block-scan path of the contains kernel for rows of at least 19 bytes. External linkage is load-
// bearing: it keeps interprocedural range propagation from destabilizing the auto-vectorized block
// loop (see simd_utils.cpp).
bool ContainsLong(const char *p, uint32_t len, uint32_t target);

} // namespace duckdb
