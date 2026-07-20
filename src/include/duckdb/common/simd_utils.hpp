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

namespace duckdb {

//! Width in bytes of a single comparison code (the window the byte-scan kernels match at a time).
constexpr uint32_t CODE_LEN = sizeof(uint32_t);

// Prefilter driver. Scans `count` candidate rows (row i is `data[i]`, `lengths[i]` bytes) for the
// CODE_LEN-byte `pattern`, compacting matches back into `sel` in place (write position never runs
// ahead of the read position) and returning the match count. The caller must ensure every row is
// >= CODE_LEN bytes.
idx_t k_vert_u32(const char *const *data, const uint32_t *lengths, SelectionVector &sel, idx_t count,
                 const char *pattern);

// Block-scan path of the contains kernel for rows of at least 19 bytes. External linkage is load-
// bearing: it keeps interprocedural range propagation from destabilizing the auto-vectorized block
// loop (see simd_utils.cpp).
bool ContainsLong(const char *p, uint32_t len, uint32_t target);

} // namespace duckdb
