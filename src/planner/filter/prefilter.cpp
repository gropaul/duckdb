#include "duckdb/planner/filter/prefilter.hpp"

#include "duckdb/common/exception.hpp"
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
// Two spellings of the block scan. VEXT states the vector operation with GNU vector extensions,
// which lowers to NEON on aarch64 and SSE2 on x86-64 from one source (both baseline ISAs, so no
// -march and no runtime dispatch). SCALAR is plain C++ left to the auto-vectorizer, and is what
// MSVC and any other non-GNU compiler gets. The spelling decides where multi-target fusion is
// possible: VEXT loads a block's phase vectors once and ORs all K targets into shared
// accumulators, so a marginal target costs compares rather than another pass over the bytes;
// SCALAR can only fuse at row level (one target scanned to completion, then the next) and its cost
// grows linearly in the target count.
//
// CODEGEN WARNING (the price of portability): the SCALAR hot loop relies on the auto-vectorizer.
// The intended shape for the block scan is, per 16 start positions, CODE_LEN contiguous 16-byte
// loads + that many byte compares + ANDs. Scanning CodeT-sized windows instead (the obvious
// formulation) tempts the vectorizer into an interleaved-access group (ld4) that measured 2-3x
// slower, and a cross-block vector accumulator scalarizes the 4-byte-code case - both avoided by
// the byte domain and the per-block bool reduce below, which is why the kernel lives in this
// dedicated .cpp. After touching it: disassemble the instantiated kernels and check for ld4/st4,
// scalar byte loads (ldrb) or stack spills in the inner loop, or the absence of vector compares.
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
//   PHASES      phase loads sweeping a block's 16 starts as CodeT lanes (2 / 4)
//   LANES       CodeT lanes in one 16-byte vector (8 / 4)
template <class CodeT>
struct Geometry {
	static constexpr uint32_t CODE_LEN = sizeof(CodeT);
	static constexpr uint32_t WINDOWS_16 = 16;
	static constexpr uint32_t MIN_LEN_16 = WINDOWS_16 + CODE_LEN - 1;
	static constexpr uint32_t MIN_LEN_8 = sizeof(uint64_t);
	static constexpr uint32_t WINDOWS_U64 = static_cast<uint32_t>(sizeof(uint64_t)) - CODE_LEN + 1;
	static constexpr uint32_t TINY_ITERS = MIN_LEN_8 - CODE_LEN;
	static constexpr uint32_t PHASES = CODE_LEN;
	static constexpr uint32_t LANES = WINDOWS_16 / CODE_LEN;
	static_assert(sizeof(CodeT) == 2 || sizeof(CodeT) == 4, "prefilter supports 2- and 4-byte codes");
	static_assert(PHASES * LANES == WINDOWS_16, "block sweep does not cover 16 starts");
	static_assert(3 * WINDOWS_U64 >= WINDOWS_16 - 1, "the mid path leaves a window uncovered");
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

//===--------------------------------------------------------------------===//
// Row tests
//===--------------------------------------------------------------------===//
// A row test owns the per-call preparation of the targets (splatting them into vectors, or just
// copying them out of the caller's array) and answers "does this row contain any target". The
// drivers below are written against the interface, so the two layouts do not each need a copy of
// every spelling. Prep is built once per call, never per row.

//! Scalar row test: the targets themselves, scanned one at a time with an early exit.
template <class CodeT>
struct ScalarRowTest {
	using G = Geometry<CodeT>;

	struct Prep {
		const CodeT *targets;
		idx_t target_count;
	};

	static Prep Make(const CodeT *targets, idx_t target_count) {
		return {targets, target_count};
	}

	// The length dispatch runs once per row; the target loop wraps the length-specialized scan and
	// exits on the first hit.
	static inline bool Contains(const char *row, uint32_t len, const Prep &prep) {
		bool hit = false;
		if (len >= G::MIN_LEN_16) {
			for (idx_t t = 0; t < prep.target_count && !hit; ++t) {
				hit = ContainsLong(row, len, prep.targets[t]);
			}
		} else if (len >= G::MIN_LEN_8) {
			for (idx_t t = 0; t < prep.target_count && !hit; ++t) {
				hit = ContainsMid(row, len, prep.targets[t]);
			}
		} else {
			for (idx_t t = 0; t < prep.target_count && !hit; ++t) {
				hit = ContainsTiny(row, len, prep.targets[t]);
			}
		}
		return hit;
	}
};

#if defined(__GNUC__) || defined(__clang__)
#define DUCKDB_PREFILTER_VEXT 1
#endif

#ifdef DUCKDB_PREFILTER_VEXT

// GNU vector extensions: understood identically by gcc and clang, and lowered to NEON on aarch64
// and SSE2 on x86-64 - both baseline ISAs, so this needs no -march and no runtime dispatch. 16
// bytes wide on purpose: the same algorithm on both targets, not a wider one.
typedef uint16_t prefilter_v8u16 __attribute__((vector_size(16)));
typedef uint32_t prefilter_v4u32 __attribute__((vector_size(16)));

template <class CodeT>
struct VectorOf;
template <>
struct VectorOf<uint16_t> {
	using type = prefilter_v8u16;
};
template <>
struct VectorOf<uint32_t> {
	using type = prefilter_v4u32;
};

//! Vext row test. K is a compile-time parameter so the target loops unroll and prep.splat[j]
//! becomes a fixed index - that is what makes block-level fusion pay.
template <class CodeT, uint32_t K>
struct VextRowTest {
	using G = Geometry<CodeT>;
	using V = typename VectorOf<CodeT>::type;

	struct Prep {
		V splat[K];
		CodeT codes[K];
	};

	static Prep Make(const CodeT *targets) {
		Prep prep;
		for (uint32_t j = 0; j < K; ++j) {
			V splat;
			for (uint32_t lane = 0; lane < G::LANES; ++lane) {
				splat[lane] = targets[j];
			}
			prep.splat[j] = splat;
			prep.codes[j] = targets[j];
		}
		return prep;
	}

	// One block: the 16 window starts as PHASES phase loads (4 of 4 u32 lanes at CODE_LEN 4; 2 of
	// 8 u16 lanes at CODE_LEN 2), each compared against every target and OR-ed into a shared
	// accumulator. The phase loads happen ONCE per block regardless of K - that is the fusion, and
	// it is why a marginal target costs its compares rather than another pass over the bytes.
	// Reads MIN_LEN_16 bytes.
	static inline V BlockAccumulate(const char *__restrict block, const Prep &prep) {
		V windows[G::PHASES];
		for (uint32_t phase = 0; phase < G::PHASES; ++phase) {
			std::memcpy(&windows[phase], block + phase, sizeof(V)); // portable unaligned load
		}
		V acc = V {};
		for (uint32_t j = 0; j < K; ++j) { // K is constant: unrolls
			for (uint32_t phase = 0; phase < G::PHASES; ++phase) {
				acc |= (windows[phase] == prep.splat[j]);
			}
		}
		return acc;
	}

	// Reinterpreting to two u64 lanes keeps the reduce to one OR and two extracts, which is what
	// both compilers lower it to.
	static inline bool AnyLaneSet(V acc) {
		uint64_t halves[2];
		std::memcpy(halves, &acc, sizeof(halves));
		return (halves[0] | halves[1]) != 0;
	}

	// len >= MIN_LEN_16: full blocks, then one block re-aligned to the row end. The accumulator
	// stays vertical across the whole row, so the horizontal reduce happens once per row rather
	// than once per block.
	static inline bool ContainsLongAny(const char *row, uint32_t len, const Prep &prep) {
		const uint32_t num_windows = len - G::CODE_LEN + 1;
		V acc = V {};
		uint32_t start = 0;
		for (; start + G::WINDOWS_16 <= num_windows; start += G::WINDOWS_16) {
			acc |= BlockAccumulate(row + start, prep);
		}
		if (start < num_windows) {
			acc |= BlockAccumulate(row + (num_windows - G::WINDOWS_16), prep);
		}
		return AnyLaneSet(acc);
	}

	// Only the widest length class differs per spelling - a 16-byte vector has nothing to do on an
	// 8-byte row, so the narrow classes reuse the scalar word loads.
	static inline bool Contains(const char *row, uint32_t len, const Prep &prep) {
		if (len >= G::MIN_LEN_16) {
			return ContainsLongAny(row, len, prep);
		}
		bool hit = false;
		if (len >= G::MIN_LEN_8) {
			for (uint32_t j = 0; j < K && !hit; ++j) {
				hit = ContainsMid(row, len, prep.codes[j]);
			}
		} else {
			for (uint32_t j = 0; j < K && !hit; ++j) {
				hit = ContainsTiny(row, len, prep.codes[j]);
			}
		}
		return hit;
	}
};

//! Above this the fused kernel's register pressure stops paying and the scalar spelling takes over.
constexpr idx_t MAX_FUSED_TARGETS = 8;

#endif

//===--------------------------------------------------------------------===//
// Drivers
//===--------------------------------------------------------------------===//

template <class CodeT, class Test>
idx_t RunStrings(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                 SelectionVector &result_sel, idx_t count, const typename Test::Prep &prep) {
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
		const bool hit = Test::Contains(str.GetData(), len, prep);
		result_sel.set_index(result_count, sel_idx);
		result_count += hit;
	}
	return result_count;
}

template <class CodeT, class Test>
idx_t RunView(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
              SelectionVector &result_sel, idx_t count, const typename Test::Prep &prep) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto sel_idx = sel.get_index(i);
		const auto len = view.lengths[sel_idx];
		if (validity.RowIsValid(sel_idx) && len >= Geometry<CodeT>::CODE_LEN) {
			const auto row = view.base + view.offsets[sel_idx];
			const bool hit = Test::Contains(row, len, prep);
			result_sel.set_index(result_count, sel_idx);
			result_count += hit;
		}
	}
	return result_count;
}

//===--------------------------------------------------------------------===//
// Dispatch
//===--------------------------------------------------------------------===//

#ifdef DUCKDB_PREFILTER_VEXT

// Every fused kernel takes its target count K as a compile-time parameter, so turning the caller's
// runtime count into that parameter needs a switch over 1..MAX_FUSED_TARGETS. This is the only
// place that writes one; both layouts share it through the body template. The bodies are noinline
// so the eight arms are register-allocated separately - otherwise the K=8 spill set lands on the
// K=1 arm - at the cost of one call per vector.
template <class CodeT, template <class, uint32_t> class Body, class... Args>
inline idx_t DispatchTargetCount(idx_t target_count, Args... args) {
	switch (target_count) {
	case 1:
		return Body<CodeT, 1>::Run(args...);
	case 2:
		return Body<CodeT, 2>::Run(args...);
	case 3:
		return Body<CodeT, 3>::Run(args...);
	case 4:
		return Body<CodeT, 4>::Run(args...);
	case 5:
		return Body<CodeT, 5>::Run(args...);
	case 6:
		return Body<CodeT, 6>::Run(args...);
	case 7:
		return Body<CodeT, 7>::Run(args...);
	case 8:
		return Body<CodeT, 8>::Run(args...);
	default:
		// out of contract: clamping would read targets[8..count) and returning 0 would be a silent
		// false negative, the one failure mode a prefilter may never have
		throw InternalException("prefilter dispatched with %llu targets, at most %llu are fused",
		                        static_cast<uint64_t>(target_count), static_cast<uint64_t>(MAX_FUSED_TARGETS));
	}
}

template <class CodeT, uint32_t K>
struct StringsVextBody {
	__attribute__((noinline)) static idx_t Run(const string_t *strings, const ValidityMask *validity,
	                                           const SelectionVector *sel, SelectionVector *result_sel, idx_t count,
	                                           const CodeT *targets) {
		using Test = VextRowTest<CodeT, K>;
		const auto prep = Test::Make(targets);
		return RunStrings<CodeT, Test>(strings, *validity, *sel, *result_sel, count, prep);
	}
};

template <class CodeT, uint32_t K>
struct ViewVextBody {
	__attribute__((noinline)) static idx_t Run(const var_binary_view_t *view, const ValidityMask *validity,
	                                           const SelectionVector *sel, SelectionVector *result_sel, idx_t count,
	                                           const CodeT *targets) {
		using Test = VextRowTest<CodeT, K>;
		const auto prep = Test::Make(targets);
		return RunView<CodeT, Test>(*view, *validity, *sel, *result_sel, count, prep);
	}
};

#endif

template <class CodeT>
idx_t PrefilterStringsImpl(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const CodeT *targets, idx_t target_count) {
	if (target_count == 0) {
		return 0;
	}
#ifdef DUCKDB_PREFILTER_VEXT
	if (target_count <= MAX_FUSED_TARGETS) {
		return DispatchTargetCount<CodeT, StringsVextBody>(target_count, strings, &validity, &sel, &result_sel, count,
		                                                   targets);
	}
#endif
	using Test = ScalarRowTest<CodeT>;
	return RunStrings<CodeT, Test>(strings, validity, sel, result_sel, count, Test::Make(targets, target_count));
}

template <class CodeT>
idx_t PrefilterViewImpl(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                        SelectionVector &result_sel, idx_t count, const CodeT *targets, idx_t target_count) {
	if (target_count == 0) {
		return 0;
	}
#ifdef DUCKDB_PREFILTER_VEXT
	if (target_count <= MAX_FUSED_TARGETS) {
		return DispatchTargetCount<CodeT, ViewVextBody>(target_count, &view, &validity, &sel, &result_sel, count,
		                                                targets);
	}
#endif
	using Test = ScalarRowTest<CodeT>;
	return RunView<CodeT, Test>(view, validity, sel, result_sel, count, Test::Make(targets, target_count));
}

} // namespace

idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count) {
	return PrefilterStringsImpl(strings, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const string_t *strings, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count) {
	return PrefilterStringsImpl(strings, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint32_t *targets, idx_t target_count) {
	return PrefilterViewImpl(view, validity, sel, result_sel, count, targets, target_count);
}

idx_t PrefilterContainsAny(const var_binary_view_t &view, const ValidityMask &validity, const SelectionVector &sel,
                           SelectionVector &result_sel, idx_t count, const uint16_t *targets, idx_t target_count) {
	return PrefilterViewImpl(view, validity, sel, result_sel, count, targets, target_count);
}

} // namespace duckdb
