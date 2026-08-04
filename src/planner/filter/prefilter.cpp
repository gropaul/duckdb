#include "duckdb/planner/filter/prefilter.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <cstring>

namespace duckdb {

// Byte-domain contains kernel. Long rows are scanned a block of 16 start positions at a time: for
// each needle byte k the compare (row[start + k] == code_byte[k]) is a contiguous 16-byte load vs
// a broadcast byte, AND-ed across the code's bytes so a lane survives only where the whole code
// starts. "Contains" is idempotent under overlap, so leftover windows are covered by re-running one
// full block aligned to the row end - no scalar tail. Short rows use clamped overlapping CodeT-word
// loads. All paths avoid length-dependent inner loops (whose exit branches mispredict on
// random-length rows) and never read a byte outside the row.
//
// CODEGEN WARNING (the price of portability): the hot loops rely on the auto-vectorizer. The
// intended shape for the block scan is, per 16 start positions, CODE_LEN contiguous 16-byte loads +
// that many byte compares + ANDs. Scanning CodeT-sized windows instead (the obvious formulation)
// tempts the vectorizer into an interleaved-access group (ld4) that measured 2-3x slower, and a
// cross-block vector accumulator scalarizes the 4-byte-code case - both avoided by the byte domain
// and the per-block bool reduce below, which is why the kernel lives in this dedicated .cpp. After
// touching it: disassemble the instantiated kernels and check for ld4/st4, scalar byte loads
// (ldrb) or stack spills in the inner loop, or the absence of vector compares.
namespace {

// Width-dependent geometry. CodeT is uint16_t or uint32_t; everything derives from its size, so
// both widths share one implementation.
//
//   CODE_LEN    bytes per code (2 / 4)
//   WINDOWS_16  window starts checked per 16-byte block (16 for both)
//   MIN_LEN_16  shortest row for the block path: a block's last window ends at
//               byte 16 + CODE_LEN - 2 (17 / 19 bytes read)
//   WINDOWS_U64 window starts inside one 8-byte word (7 / 5)
//   TINY_ITERS  clamped loads covering every window of a row shorter than 8
//               bytes: last start is len - CODE_LEN <= 8 - CODE_LEN - 1 (6 / 4)
template <class CodeT>
struct Geometry {
	static constexpr uint32_t CODE_LEN = sizeof(CodeT);
	static constexpr uint32_t WINDOWS_16 = 16;
	static constexpr uint32_t MIN_LEN_16 = WINDOWS_16 + CODE_LEN - 1;
	static constexpr uint32_t MIN_LEN_8 = sizeof(uint64_t);
	static constexpr uint32_t WINDOWS_U64 = static_cast<uint32_t>(sizeof(uint64_t)) - CODE_LEN + 1;
	static constexpr uint32_t TINY_ITERS = MIN_LEN_8 - CODE_LEN;
	static_assert(sizeof(CodeT) == 2 || sizeof(CodeT) == 4, "prefilter supports 2- and 4-byte codes");
};

// Byte-domain block: the 16 consecutive start positions at `block`. For each needle byte k,
// (block[start + k] == cb[k]) is a contiguous 16-byte load compared against a broadcast byte,
// folded branchlessly into a 0x00/0xFF mask; AND-ing across k leaves 0xFF where the whole code
// starts. Reads MIN_LEN_16 bytes. Every load is a contiguous byte vector (no strided CodeT
// window), so the auto-vectorizer emits plain loads + byte compares - a CodeT-window scan tempts
// it into an interleaved-access group (ld4) that is measurably slower. Returns whether any of the
// 16 starts matched.
template <class CodeT>
inline bool ByteBlockAny(const char *__restrict block, const uint8_t *__restrict cb) {
	using G = Geometry<CodeT>;
	uint8_t mask[16];
	for (uint32_t lane = 0; lane < 16; ++lane) {
		mask[lane] = 0xFF;
	}
	for (uint32_t k = 0; k < G::CODE_LEN; ++k) {
		for (uint32_t lane = 0; lane < 16; ++lane) {
			const uint8_t eq =
			    static_cast<uint8_t>(0) - static_cast<uint8_t>(static_cast<uint8_t>(block[k + lane]) == cb[k]);
			mask[lane] &= eq;
		}
	}
	uint8_t any = 0;
	for (uint32_t lane = 0; lane < 16; ++lane) {
		any |= mask[lane];
	}
	return any != 0;
}

// len >= MIN_LEN_16: full blocks, then one block re-aligned to the row end to cover the leftover
// windows (re-checking a window is harmless for membership). Each block reduces to a single bool
// so no cross-block vector accumulator survives the loop - that shape is what the vectorizer keeps
// as byte compares instead of scalarizing the 4-byte-code case.
template <class CodeT>
bool ContainsLong(const char *row, uint32_t len, CodeT code) {
	using G = Geometry<CodeT>;
	uint8_t cb[G::CODE_LEN];
	std::memcpy(cb, &code, G::CODE_LEN);
	const uint32_t num_windows = len - G::CODE_LEN + 1;
	bool found = false;
	uint32_t start = 0;
	for (; start + G::WINDOWS_16 <= num_windows; start += G::WINDOWS_16) {
		found |= ByteBlockAny<CodeT>(row + start, cb);
	}
	if (start < num_windows) {
		found |= ByteBlockAny<CodeT>(row + (num_windows - G::WINDOWS_16), cb);
	}
	return found;
}

// The WINDOWS_U64 window starts inside the 8-byte word at block8. The caller guarantees
// block8[0..7] is inside the row, which also makes all window starts valid.
template <class CodeT>
inline uint32_t WindowsU64(const char *__restrict block8, CodeT code) {
	using G = Geometry<CodeT>;
	uint64_t word;
	std::memcpy(&word, block8, sizeof(uint64_t));
	uint32_t matched = 0;
	for (uint32_t i = 0; i < G::WINDOWS_U64; ++i) {
		matched |= (static_cast<CodeT>(word >> (8 * i)) == code) ? 1u : 0u;
	}
	return matched;
}

// len in [8, MIN_LEN_16): cover all window starts with three u64 words stepped WINDOWS_U64 apart,
// each clamped so its load ends inside the row (overlap re-checks windows, harmless).
template <class CodeT>
inline bool ContainsMid(const char *__restrict row, uint32_t len, CodeT code) {
	using G = Geometry<CodeT>;
	const uint32_t last_load = len - static_cast<uint32_t>(sizeof(uint64_t));
	uint32_t matched = WindowsU64(row, code);
	matched |= WindowsU64(row + MinValue<uint32_t>(1 * G::WINDOWS_U64, last_load), code);
	matched |= WindowsU64(row + MinValue<uint32_t>(2 * G::WINDOWS_U64, last_load), code);
	return matched != 0;
}

// len in [CODE_LEN, 8): one window per load, fixed trip count with the start clamped to the last
// valid window (re-checks, harmless). The fixed count is deliberate: a length-dependent exit
// branch mispredicts on random-length rows (measured 2.4x slower).
template <class CodeT>
inline bool ContainsTiny(const char *__restrict row, uint32_t len, CodeT code) {
	using G = Geometry<CodeT>;
	const uint32_t last_offset = len - G::CODE_LEN;
	uint32_t matched = 0;
	for (uint32_t i = 0; i < G::TINY_ITERS; ++i) {
		CodeT word;
		std::memcpy(&word, row + MinValue<uint32_t>(i, last_offset), G::CODE_LEN);
		matched |= (word == code) ? 1u : 0u;
	}
	return matched != 0;
}

// Does row[len >= CODE_LEN] contain ANY of the targets? The length dispatch runs once per row; the
// target loop wraps the length-specialized scan and exits on the first hit.
template <class CodeT>
inline bool ContainsAnyRow(const char *row, uint32_t len, const CodeT *targets, idx_t target_count) {
	using G = Geometry<CodeT>;
	bool hit = false;
	if (len >= G::MIN_LEN_16) {
		for (idx_t t = 0; t < target_count && !hit; ++t) {
			hit = ContainsLong(row, len, targets[t]);
		}
	} else if (len >= G::MIN_LEN_8) {
		for (idx_t t = 0; t < target_count && !hit; ++t) {
			hit = ContainsMid(row, len, targets[t]);
		}
	} else {
		for (idx_t t = 0; t < target_count && !hit; ++t) {
			hit = ContainsTiny(row, len, targets[t]);
		}
	}
	return hit;
}

template <class CodeT>
idx_t PrefilterContainsAnyImpl(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                               SelectionVector &result_sel, idx_t count, const CodeT *targets, idx_t target_count) {
	// pass 1: predicated prefilter - compact valid rows long enough for the kernel into result_sel.
	// branchless append: the validity/length predicate mispredicts enough at nontrivial selectivity
	// to beat a conditional store.
	idx_t candidate_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto sel_idx = sel.get_index(i);
		const auto len = UnsafeNumericCast<uint32_t>(strings[sel_idx].GetSize());
		const bool keep = validity.RowIsValid(sel_idx) && len >= Geometry<CodeT>::CODE_LEN;
		result_sel.set_index(candidate_count, sel_idx);
		candidate_count += keep;
	}
	// pass 2: scan every surviving candidate. matches compact back into result_sel in place (the
	// write position never runs ahead of the read position).
	idx_t result_count = 0;
	for (idx_t i = 0; i < candidate_count; ++i) {
		const auto sel_idx = result_sel.get_index(i);
		const auto &str = strings[sel_idx];
		const auto len = UnsafeNumericCast<uint32_t>(str.GetSize());
		const bool hit = ContainsAnyRow(str.GetData(), len, targets, target_count);
		result_sel.set_index(result_count, sel_idx);
		result_count += hit;
	}
	return result_count;
}

template <class CodeT>
idx_t PrefilterContainsAnyImpl(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                               SelectionVector &result_sel, idx_t count, const CodeT *targets, idx_t target_count) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto sel_idx = sel.get_index(i);
		const auto len = view.lengths[sel_idx];
		if (validity.RowIsValid(sel_idx) && len >= Geometry<CodeT>::CODE_LEN) {
			const auto row = view.base + view.offsets[sel_idx];
			const bool hit = ContainsAnyRow(row, len, targets, target_count);
			result_sel.set_index(result_count, sel_idx);
			result_count += hit;
		}
	}
	return result_count;
}

} // namespace

idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count) {
	return PrefilterContainsAnyImpl(strings, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count) {
	return PrefilterContainsAnyImpl(strings, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count) {
	return PrefilterContainsAnyImpl(view, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count) {
	return PrefilterContainsAnyImpl(view, validity, sel, result_sel, count, targets, target_count);
}

} // namespace duckdb
