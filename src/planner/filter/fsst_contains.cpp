#include "duckdb/planner/filter/fsst_contains.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "fsst.h"

// The kernels of search/prefilter need NEON, AVX-512BW or AVX2 and refuse to compile without one.
// A build below that baseline evaluates contains on decompressed strings rather than failing to build;
// on x86 that means configuring with -march=native (or -mavx2) to get the compressed scan at all.
#if defined(__ARM_NEON) || defined(__AVX512BW__) || defined(__AVX2__)
#define DUCKDB_FSST_SEARCH_AVAILABLE
#include "prefilter/prefilter.hpp"
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
	search::Analysis analysis;
	//! Anchored needles are answered per row, with no scan over the code stream and no stream offsets
	search::Anchor anchor = search::Anchor::None;
	//! Filter keeps the valid rows that do not match
	bool negated = false;
	//! Per vector row, whether it matched
	unsafe_vector<uint8_t> matched;

	unsafe_vector<uint32_t> stream_offsets;
	//! The stream rows the search found, ascending
	std::vector<size_t> found;
	//! A partial selection as ascending stream rows, and the scans restricted to it
	std::vector<uint32_t> stream_sel;
	search::Finder finder {STANDARD_VECTOR_SIZE};
};

FSSTContains::FSSTContains() : state(make_uniq<State>()) {
}

FSSTContains::~FSSTContains() {
}

bool FSSTContains::Prepare(idx_t row_group_id, const void *decoder_p, idx_t symbol_count, const FSSTPattern &pattern,
                           idx_t row_count) {
	if (row_group_id != 0 && row_group_id == state->planned_row_group) {
		return state->usable;
	}
	state->planned_row_group = row_group_id;
	state->negated = pattern.negated;
	state->usable = false;
	if (!decoder_p || symbol_count == 0 || !pattern.IsExact()) {
		return false;
	}
	const auto &decoder = *static_cast<const fsst_decoder_t *>(decoder_p);
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
	state->anchor = search::Anchor::None;
	if (pattern.anchor_start) {
		state->anchor = search::Anchor::Start;
	} else if (pattern.anchor_end) {
		state->anchor = search::Anchor::End;
	}
	auto &literal = pattern.literals[0];
	state->analysis = search::analyze(const_data_ptr_cast(literal.data()), literal.size(), state->dict, state->freq,
	                                  row_count, state->anchor);
	if (state->analysis.matches_all || (state->anchor == search::Anchor::None && state->analysis.cover.empty())) {
		// every row, or a literal this symbol table cannot spell: left to the decompressing path
		return false;
	}
	state->usable = true;
	return true;
}

bool FSSTContains::Filter(const var_binary_view_t &view, const ValidityMask &validity, idx_t row_count,
                          const SelectionVector &sel, idx_t count, SelectionVector &result_sel, idx_t &result_count) {
	if (!FilterMatches(view, validity, row_count, sel, count, result_sel, result_count)) {
		return false;
	}
	if (!state->negated) {
		return true;
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
		                                                state->dict, state->analysis.walk, result_sel.data());
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

	auto &found = state->found;
	found.clear();
	if (count < row_count) {
		// a partial selection is scanned over its own rows only, in stream order
		auto &stream_sel = state->stream_sel;
		stream_sel.resize(count);
		for (idx_t i = 0; i < count; i++) {
			stream_sel[count - 1 - i] = UnsafeNumericCast<uint32_t>(row_count - 1 - sel.get_index(i));
		}
		state->finder.find(codes, stream_size, stream_offsets.data(), row_count + 1, state->dict, state->analysis,
		                   stream_sel.data(), count, found);
	} else {
		search::candidate_rows(codes, stream_size, stream_offsets.data(), row_count + 1, state->dict, state->analysis,
		                       found);
	}
	auto &matched = state->matched;
	matched.assign(row_count, 0);
	for (auto stream_row : found) {
		matched[row_count - 1 - stream_row] = 1;
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

bool FSSTContains::Prepare(idx_t, const void *, idx_t, const FSSTPattern &, idx_t) {
	return false;
}

bool FSSTContains::Filter(const var_binary_view_t &, const ValidityMask &, idx_t, const SelectionVector &, idx_t,
                          SelectionVector &, idx_t &) {
	return false;
}

#endif

} // namespace duckdb
