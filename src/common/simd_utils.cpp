#include "duckdb/common/simd_utils.hpp"

#include "duckdb/common/helper.hpp"

#include <cstring>

namespace duckdb {

// Vertical-accumulator contains kernel (vert_u32). Long rows compare 16 window starts per block,
// OR-ing each window's match mask into its own accumulator (vertical OR) so the horizontal reduce
// happens once per row instead of once per block. "Contains" is idempotent under overlap, so the
// leftover windows are covered by re-running one full block aligned to the row end - no scalar tail.
// Short rows use clamped overlapping loads for the same reason. All paths avoid length-dependent
// inner loops (whose exit branches mispredict on random-length rows) and never read a byte outside
// the row, so the string buffer needs no padding contract.
namespace {

// Rows shorter than this cannot fit one block16 (it reads BLOCK_WINDOWS + CODE_LEN - 1 bytes);
// they take the short-row paths below.
constexpr uint32_t BLOCK_WINDOWS = 16;
constexpr uint32_t MIN_BLOCK_LEN = BLOCK_WINDOWS + CODE_LEN - 1; // 19

// Window starts covered by one u64 load in ContainsMid.
constexpr uint32_t U64_WINDOWS = sizeof(uint64_t) - CODE_LEN + 1; // 5

// Compare the BLOCK_WINDOWS windows starting at p+0..p+15 against `target`, OR-ing the per-window
// masks vertically into acc[0..15]. Reads p[0..18]. For each base offset the 4 loads are 16
// contiguous bytes at stride 4 - one unaligned vector load + lane compare + OR once vectorized.
inline void Block16(const char *__restrict p, uint32_t target, uint32_t *__restrict acc) {
	for (uint32_t off = 0; off < 4; ++off) {
		for (uint32_t k = 0; k < 4; ++k) {
			uint32_t w;
			std::memcpy(&w, p + off + 4 * k, 4);
			acc[4 * off + k] |= (w == target) ? 0xFFFFFFFFu : 0u;
		}
	}
}

} // namespace

// len >= MIN_BLOCK_LEN: full blocks, then one block re-aligned to the row end to cover the leftover
// windows (overlapping the previous block is fine).
// noinline + external linkage pin the intended block-loop codegen (per 16-byte block: 4 vector
// loads, 4 compares, 4 ORs). With internal linkage, clang propagates the caller's len >= 19 range
// into the standalone body (IPA runs even under noinline) and the unroller + SLP vectorizer rewrite
// the loop with interleaved gathers and a big per-row reduction - measured ~2x slower on mid/large
// rows. The call costs ~a cycle per long row, noise at >= 19 bytes/row.
inline bool ContainsLong(const char *p, uint32_t len, uint32_t target) {
	const uint32_t nwin = len - CODE_LEN + 1;
	uint32_t acc[BLOCK_WINDOWS] = {0};
	uint32_t j = 0;
	for (; j + BLOCK_WINDOWS <= nwin; j += BLOCK_WINDOWS) {
		Block16(p + j, target, acc);
	}
	if (j < nwin) {
		Block16(p + (nwin - BLOCK_WINDOWS), target, acc);
	}
	uint32_t r = 0;
	for (uint32_t k = 0; k < BLOCK_WINDOWS; ++k) {
		r |= acc[k];
	}
	return r != 0;
}

namespace {

// Do the U64_WINDOWS windows inside the 8-byte word at p contain the code? Caller guarantees
// p[0..7] is inside the row, which also makes all 5 window starts valid.
inline uint32_t Swar5(const char *__restrict p, uint32_t target) {
	uint64_t w;
	std::memcpy(&w, p, sizeof(uint64_t));
	uint32_t r = 0;
	for (uint32_t k = 0; k < U64_WINDOWS; ++k) {
		r |= (static_cast<uint32_t>(w >> (8 * k)) == target) ? 1u : 0u;
	}
	return r;
}

// len in [8,18]: 5..15 windows via three overlapping Swar5 blocks. The block starts are clamped so
// the last u64 load ends exactly at the row end.
inline bool ContainsMid(const char *__restrict p, uint32_t len, uint32_t target) {
	const uint32_t last_load = len - sizeof(uint64_t); // last in-bounds u64 offset
	uint32_t r = Swar5(p, target);
	r |= Swar5(p + MinValue<uint32_t>(1 * U64_WINDOWS, last_load), target);
	r |= Swar5(p + MinValue<uint32_t>(2 * U64_WINDOWS, last_load), target);
	return r != 0;
}

// len in [4,7]: always check 4 windows, clamping the start index so the out-of-range ones just
// re-check the last valid window. The fixed trip count keeps it branchless - a length-dependent
// loop mispredicts its exit branch on random-length rows.
inline bool ContainsTiny(const char *__restrict p, uint32_t len, uint32_t target) {
	const uint32_t last = len - CODE_LEN; // last valid window start
	uint32_t r = 0;
	for (uint32_t j = 0; j < 4; ++j) {
		uint32_t w;
		std::memcpy(&w, p + MinValue<uint32_t>(j, last), CODE_LEN);
		r |= (w == target) ? 1u : 0u;
	}
	return r != 0;
}

// Does the 4-byte code occur anywhere in p[0..len)? Requires len >= CODE_LEN.
bool ContainsU32(const char *__restrict p, uint32_t len, uint32_t target) {
	if (len >= MIN_BLOCK_LEN) {
		return ContainsLong(p, len, target);
	}
	if (len >= sizeof(uint64_t)) {
		return ContainsMid(p, len, target);
	}
	return ContainsTiny(p, len, target);
}

} // namespace

// noinline so the byte-scan fast path shows up as its own frame in a flamegraph.
idx_t k_vert_u32(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                 SelectionVector &result_sel, idx_t count, const char *pattern) {
	uint32_t target;
	std::memcpy(&target, pattern, CODE_LEN);
	// pass 1: predicated prefilter - compact valid rows long enough for ContainsU32 (len >= CODE_LEN) into
	// result_sel. branchless append: the validity/length predicate mispredicts enough at nontrivial selectivity
	// to beat a conditional store.
	idx_t candidate_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto sel_idx = sel.get_index(i);
		const auto len = UnsafeNumericCast<uint32_t>(strings[sel_idx].GetSize());
		const bool keep = validity.RowIsValid(sel_idx) && len >= CODE_LEN;
		result_sel.set_index(candidate_count, sel_idx);
		candidate_count += keep;
	}
	// pass 2: scan the surviving candidates for the CODE_LEN-byte code, compacting matches back into result_sel
	// in place (write position never runs ahead of the read position).
	idx_t result_count = 0;
	for (idx_t i = 0; i < candidate_count; ++i) {
		const auto sel_idx = result_sel.get_index(i);
		const auto &str = strings[sel_idx];
		result_sel.set_index(result_count, sel_idx);
		result_count += ContainsU32(str.GetData(), UnsafeNumericCast<uint32_t>(str.GetSize()), target);
	}
	return result_count;
}

// noinline so the byte-scan fast path shows up as its own frame in a flamegraph.
idx_t k_vert_u32(const char *base, const int32_t *offsets, const ValidityMask &validity, const SelectionVector &sel,
                 SelectionVector &result_sel, idx_t count, const char *pattern) {
	uint32_t target;
	std::memcpy(&target, pattern, CODE_LEN);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto sel_idx = sel.get_index(i);
		const auto len = UnsafeNumericCast<uint32_t>(offsets[sel_idx] - offsets[sel_idx + 1]);
		const bool keep = validity.RowIsValid(sel_idx) && len >= CODE_LEN;
		if (keep) {
			result_sel.set_index(result_count, sel_idx);
			const auto start = offsets[sel_idx + 1];
			result_count += ContainsU32(base + start, len, target);
		}
	}
	return result_count;
}

} // namespace duckdb
