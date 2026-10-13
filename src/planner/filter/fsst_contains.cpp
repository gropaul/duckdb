#include "duckdb/planner/filter/fsst_contains.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "fsst.h"

// The kernels of search/prefilter need NEON, AVX-512BW or AVX2 and refuse to compile without one.
// A build below that baseline evaluates contains on decompressed strings rather than failing to build;
// on x86 that means configuring with -march=native (or -mavx2) to get the compressed scan at all.
#if defined(__ARM_NEON) || defined(__AVX512BW__) || defined(__AVX2__)
#define DUCKDB_FSST_SEARCH_AVAILABLE
#include "prefilter/pattern.hpp"
#endif

namespace duckdb {

#ifdef DUCKDB_FSST_SEARCH_AVAILABLE

namespace search = fsst::search::prefilter;

struct FSSTContains::State {
	//! The row group the plan was built for, 0 when none
	idx_t planned_row_group = 0;
	bool usable = false;
	search::Dictionary dict;
	search::Frequency freq;
	//! The needles' covers merged into one: a row matching any needle holds a code of its own cover,
	//! hence one of the merged cover. The scan runs this cover once. Empty when anchored.
	search::ProbeCover cover;
	uint32_t covered_frequency = 0;
	//! One walk per needle: a hit is a match when some needle's walk accepts it. Anchored: exactly one walk
	std::vector<search::scan::Walk> walks;
	//! Anchored needles are answered per row, with no scan over the code stream and no stream offsets
	search::Anchor anchor = search::Anchor::None;
	//! One plan per OR branch, when the branches cannot share one cover: a branch with several literals, or
	//! branches of different anchoring. Each is searched on its own and a row matches when any branch finds it.
	//! Empty when the shared cover above is used.
	std::vector<search::PatternPlan> alternatives;
	//! Every alternative is anchored at the start, so each is a check per row and needs no stream
	bool all_anchored = false;
	//! The selected rows no alternative has matched yet, for the anchored alternatives
	std::vector<uint32_t> remaining;
	bool exact = true;
	//! The single pattern is negated: Filter keeps the valid rows that do not match
	bool negated = false;
	//! Per vector row, whether it matched
	unsafe_vector<uint8_t> matched;

	unsafe_vector<uint32_t> stream_offsets;
	//! The stream rows one search found, ascending
	std::vector<size_t> found;
	//! A partial selection as ascending stream rows, and the scans restricted to it
	std::vector<uint32_t> stream_sel;
	search::Finder finder {STANDARD_VECTOR_SIZE};
};

namespace {

//! The union of the covers, as the runs and pairs they are built from.
search::ProbeCover MergeCovers(const vector<search::ProbeCover> &covers) {
	std::vector<search::CodeRange> runs;
	std::vector<search::CodePair> pairs;
	for (auto &cover : covers) {
		for (auto code : cover.points) {
			runs.push_back({code, code});
		}
		for (auto &range : cover.ranges) {
			runs.push_back(range);
		}
		for (auto pair : cover.pairs) {
			pairs.push_back(pair);
		}
	}
	auto merged = search::ProbeCover::from_runs(std::move(runs));
	std::sort(pairs.begin(), pairs.end());
	pairs.erase(std::unique(pairs.begin(), pairs.end()), pairs.end());
	merged.pairs = std::move(pairs);
	return merged;
}

//! Stage-two check of the scan: the hit is a match when any needle's walk verifies it
struct AnyWalkCheck {
	const std::vector<search::scan::Walk> &walks;
	const search::Dictionary &dict;
	const uint8_t *codes;

	bool passes(size_t hit, size_t row_start, size_t row_end) const {
		for (auto &walk : walks) {
			if (walk.check(dict, codes, row_start, row_end, hit)) {
				return true;
			}
		}
		return false;
	}
};

} // namespace

FSSTContains::FSSTContains() : state(make_uniq<State>()) {
}

FSSTContains::~FSSTContains() {
}

bool FSSTContains::PlanIsExact() const {
#ifdef DUCKDB_FSST_SEARCH_AVAILABLE
	return state->exact;
#else
	return false;
#endif
}

bool FSSTContains::Prepare(idx_t row_group_id, const void *decoder_p, idx_t symbol_count,
                           const vector<FSSTPattern> &patterns, idx_t row_count) {
	if (row_group_id != 0 && row_group_id == state->planned_row_group) {
		return state->usable;
	}
	state->planned_row_group = row_group_id;
	state->negated = patterns.size() == 1 && patterns[0].negated;
	state->usable = false;
	if (!decoder_p) {
		return false;
	}
	const auto &decoder = *static_cast<const fsst_decoder_t *>(decoder_p);
	if (patterns.empty() || symbol_count == 0) {
		return false;
	}
	state->dict = search::Dictionary::from(decoder.symbol, decoder.len, symbol_count);
	if (!state->dict.sorted()) {
		// the planner requires codes in byte order of their symbols (graph.hpp asserts it); segments
		// written before fsst_sort_codes are not, and planning over them drops matching rows
		return false;
	}
	// the counts the encoder stored with the symbol table; zero where the segment predates them
	state->freq = search::Frequency::of_counts(decoder.count);
	if (state->freq.total == 0) {
		return false;
	}

	// One literal per pattern, anchored at most at one end, and one shape across all of them is the case worth
	// a single merged scan: the covers become one and a row matches when any walk accepts a hit. Anything else
	// is planned per pattern.
	bool single_literals = true;
	for (auto &pattern : patterns) {
		single_literals = single_literals && pattern.IsExact() &&
		                  pattern.anchor_start == patterns[0].anchor_start &&
		                  pattern.anchor_end == patterns[0].anchor_end;
	}
	auto anchor = search::Anchor::None;
	if (patterns[0].anchor_start) {
		anchor = search::Anchor::Start;
	} else if (patterns[0].anchor_end) {
		anchor = search::Anchor::End;
	}
	const bool anchored = anchor != search::Anchor::None;
	if (single_literals && anchored && patterns.size() > 1) {
		// an OR of anchored patterns is left to the decompressing path
		return false;
	}

	state->walks.clear();
	state->alternatives.clear();
	state->all_anchored = true;
	state->anchor = single_literals ? anchor : search::Anchor::None;
	state->exact = true;

	if (single_literals) {
		vector<search::ProbeCover> covers;
		for (auto &pattern : patterns) {
			auto &literal = pattern.literals[0];
			auto analysis = search::analyze(const_data_ptr_cast(literal.data()), literal.size(), state->dict,
			                                state->freq, row_count, anchor);
			if (analysis.matches_all || (!anchored && analysis.cover.empty())) {
				// every row, or a run this symbol table cannot spell: left to the decompressing path
				return false;
			}
			if (!anchored) {
				covers.push_back(std::move(analysis.cover));
			}
			state->walks.push_back(std::move(analysis.walk));
		}
		if (!anchored) {
			state->cover = MergeCovers(covers);
			state->covered_frequency = state->freq.of_cover(state->cover);
		}
		state->usable = true;
		return true;
	}

	for (auto &pattern : patterns) {
		auto plan = search::plan_pattern(state->dict, state->freq, pattern.literals, pattern.anchor_start,
		                                 pattern.anchor_end, row_count);
		if (!plan.usable) {
			return false;
		}
		state->exact = state->exact && plan.exact;
		state->all_anchored = state->all_anchored && plan.anchored;
		state->alternatives.push_back(std::move(plan));
	}
	state->usable = true;
	return true;
}

bool FSSTContains::Filter(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
                          const SelectionVector &sel, idx_t count, SelectionVector &result_sel, idx_t &result_count) {
	if (!state->negated) {
		return FilterMatches(view, validity, row_count, sel, count, result_sel, result_count);
	}
	// the complement of a superset would drop matching rows
	if (!state->exact || !FilterMatches(view, validity, row_count, sel, count, result_sel, result_count)) {
		return false;
	}
	auto &matched = state->matched;
	matched.assign(row_count, 0);
	for (idx_t match_index = 0; match_index < result_count; match_index++) {
		matched[result_sel.get_index(match_index)] = 1;
	}
	// a NULL row is stored empty and never matches, so validity has to drop it here
	result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const idx_t row = sel.get_index(i);
		result_sel.set_index(result_count, row);
		result_count += !matched[row] & validity.RowIsValid(row);
	}
	return true;
}

bool FSSTContains::FilterMatches(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
                                 const SelectionVector &sel, idx_t count, SelectionVector &result_sel,
                                 idx_t &result_count) {
	if (!state->usable || row_count == 0) {
		return false;
	}
	const auto codes = const_data_ptr_cast(view.base);
	if (state->anchor != search::Anchor::None) {
		// each row in sel is checked on its own, straight into result_sel; NULL rows are then dropped from it
		const idx_t match_count = search::anchored_rows(codes, view.offsets, view.lengths, sel.data(), count,
		                                                state->dict, state->walks[0], result_sel.data());
		if (validity.AllValid()) {
			result_count = match_count;
			return true;
		}
		result_count = 0;
		for (idx_t match_index = 0; match_index < match_count; match_index++) {
			const auto row = result_sel.get_index(match_index);
			result_sel.set_index(result_count, row);
			result_count += validity.RowIsValid(row);
		}
		return true;
	}
	auto &matched = state->matched;
	if (!state->alternatives.empty() && state->all_anchored) {
		// an anchored alternative is a check per row, so it only reads the selected rows no earlier one matched
		auto &remaining = state->remaining;
		remaining.resize(count);
		for (idx_t i = 0; i < count; i++) {
			remaining[i] = UnsafeNumericCast<uint32_t>(sel.get_index(i));
		}
		matched.assign(row_count, 0);
		idx_t remaining_count = count;
		for (auto &alternative : state->alternatives) {
			idx_t kept = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				const idx_t row = remaining[i];
				const idx_t start = UnsafeNumericCast<idx_t>(view.offsets[row]);
				const bool match =
				    search::anchored_row(state->dict, codes, start, start + view.lengths[row], alternative);
				matched[row] = match;
				remaining[kept] = UnsafeNumericCast<uint32_t>(row);
				kept += !match;
			}
			remaining_count = kept;
		}
		result_count = 0;
		for (idx_t i = 0; i < count; i++) {
			const idx_t row = sel.get_index(i);
			result_sel.set_index(result_count, row);
			result_count += matched[row] & validity.RowIsValid(row);
		}
		return true;
	}
	// The scan runs over the payload as one stream. It is a contiguous block holding the rows back to front,
	// so reading the offsets backwards gives the ascending row offsets the scan wants; stream row r is
	// vector row row_count-1-r.
	const idx_t stream_size = UnsafeNumericCast<idx_t>(view.offsets[0]) + view.lengths[0];
	auto &stream_offsets = state->stream_offsets;
	stream_offsets.resize(row_count + 1);
	for (idx_t row = 0; row < row_count; row++) {
		const idx_t source = row_count - 1 - row;
		const idx_t offset = UnsafeNumericCast<idx_t>(view.offsets[source]);
		if (offset + view.lengths[source] !=
		    (source == 0 ? stream_size : UnsafeNumericCast<idx_t>(view.offsets[source - 1]))) {
			// a gathered slice rather than a scanned block: the rows are not one stream
			return false;
		}
		stream_offsets[row] = UnsafeNumericCast<uint32_t>(offset);
	}
	stream_offsets[row_count] = UnsafeNumericCast<uint32_t>(stream_size);

	// a partial selection is scanned over its own rows only, in stream order
	const bool selected = count < row_count;
	auto &stream_sel = state->stream_sel;
	if (selected) {
		stream_sel.resize(count);
		for (idx_t i = 0; i < count; i++) {
			stream_sel[count - 1 - i] = UnsafeNumericCast<uint32_t>(row_count - 1 - sel.get_index(i));
		}
	}
	// every search marks the vector rows it found; a row found by several alternatives is marked once
	auto &found = state->found;
	matched.assign(row_count, 0);
	auto mark_found = [&]() {
		for (auto stream_row : found) {
			matched[row_count - 1 - stream_row] = 1;
		}
	};
	if (!state->alternatives.empty()) {
		for (auto &alternative : state->alternatives) {
			found.clear();
			if (selected) {
				search::pattern_rows(codes, stream_size, stream_offsets.data(), row_count + 1, state->dict,
				                     alternative, state->finder, stream_sel.data(), count, found);
			} else {
				search::pattern_rows(codes, stream_size, stream_offsets.data(), row_count + 1, state->dict,
				                     alternative, found);
			}
			mark_found();
		}
	} else {
		found.clear();
		if (selected) {
			state->finder.find(codes, stream_size, stream_offsets.data(), row_count + 1, state->cover,
			                   state->covered_frequency, state->freq.total,
			                   AnyWalkCheck {state->walks, state->dict, codes}, stream_sel.data(), count, found);
		} else {
			search::scan::execute(state->cover, codes, stream_size, stream_offsets.data(), row_count + 1,
			                      state->covered_frequency, state->freq.total,
			                      AnyWalkCheck {state->walks, state->dict, codes}, found);
		}
		mark_found();
	}

	// The incoming selection is ascending, since every filter compacts in order, so result_sel is too. A NULL
	// row is stored as an empty string, which the empty needle would match, so validity decides too.
	result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const idx_t row = sel.get_index(i);
		result_sel.set_index(result_count, row);
		result_count += matched[row] & validity.RowIsValid(row);
	}
	return true;
}

#else

struct FSSTContains::State {};

FSSTContains::FSSTContains() : state(make_uniq<State>()) {
}

FSSTContains::~FSSTContains() {
}

bool FSSTContains::Prepare(idx_t, const void *, idx_t, const vector<FSSTPattern> &, idx_t) {
	return false;
}

bool FSSTContains::Filter(const var_binary_view_t &, const ValidityMask &, idx_t, const SelectionVector &, idx_t,
                          SelectionVector &, idx_t &) {
	return false;
}

#endif

} // namespace duckdb
