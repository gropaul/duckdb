#include "duckdb/planner/filter/fsst_prefilter.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "fsst.h"

// The kernels of search/prefilter need NEON, AVX-512BW or AVX2 and refuse to compile without one.
// A build below that baseline keeps the prefilter switched off rather than failing to build; on x86
// that means configuring with -march=native (or -mavx2) to get any scan at all.
#if defined(__ARM_NEON) || defined(__AVX512BW__) || defined(__AVX2__)
#define DUCKDB_FSST_SEARCH_AVAILABLE
#include "prefilter/prefilter.hpp"
#endif

namespace duckdb {

#ifdef DUCKDB_FSST_SEARCH_AVAILABLE

namespace search = fsst::search::prefilter;

struct FSSTPrefilter::State {
	//! The row group the plan was built for, 0 when none
	idx_t planned_row_group = 0;
	bool usable = false;
	search::Dictionary dict;
	search::Frequency freq;
	//! The needles' covers merged into one: a row matching any needle holds a code of its own cover,
	//! hence one of the merged cover. Only the superset scan reads this, never the walk, whose
	//! exactness is per needle.
	search::Analysis analysis;

	unsafe_vector<uint32_t> stream_offsets;
	std::vector<size_t> hits;
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

} // namespace

FSSTPrefilter::FSSTPrefilter() : state(make_uniq<State>()) {
}

FSSTPrefilter::~FSSTPrefilter() {
}

bool FSSTPrefilter::Prepare(idx_t row_group_id, const void *decoder_p, idx_t symbol_count,
                            const vector<string> &needles, idx_t row_count) {
	if (row_group_id != 0 && row_group_id == state->planned_row_group) {
		return state->usable;
	}
	state->planned_row_group = row_group_id;
	state->usable = false;
	if (!decoder_p) {
		return false;
	}
	const auto &decoder = *static_cast<const fsst_decoder_t *>(decoder_p);
	if (needles.empty() || symbol_count == 0) {
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

	vector<search::ProbeCover> covers;
	for (auto &needle : needles) {
		auto analysis =
		    search::analyze(const_data_ptr_cast(needle.data()), needle.size(), state->dict, state->freq, row_count);
		if (analysis.matches_all || analysis.cover.empty()) {
			// every row, or a needle this symbol table cannot spell: no cover to scan for
			return false;
		}
		covers.push_back(std::move(analysis.cover));
	}
	auto merged = MergeCovers(covers);
	state->analysis = search::Analysis {merged, state->freq.of_cover(merged), state->freq.total, 0.0, {}, false};
	state->usable = true;
	return true;
}

bool FSSTPrefilter::Filter(const var_binary_view_t &view, idx_t row_count, const SelectionVector &sel, idx_t count,
                           SelectionVector &result_sel, idx_t &result_count) {
	if (!state->usable || row_count == 0) {
		return false;
	}
	// The vector's payload is one contiguous block holding the rows back to front, so reading the offsets
	// backwards gives the ascending row offsets the scan wants; stream row r is vector row row_count-1-r.
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

	state->hits.clear();
	search::superset_rows(const_data_ptr_cast(view.base), stream_size, stream_offsets.data(), row_count + 1,
	                      state->analysis, state->hits);

	// The scan emits stream rows ascending, which is vector rows descending, so the hits walked backwards
	// merge straight into the incoming selection - itself ascending, since every filter compacts in order.
	result_count = 0;
	idx_t remaining = state->hits.size();
	idx_t i = 0;
	while (i < count && remaining > 0) {
		const idx_t row = sel.get_index(i);
		const idx_t hit_row = row_count - 1 - state->hits[remaining - 1];
		if (hit_row < row) {
			remaining--;
		} else if (hit_row > row) {
			i++;
		} else {
			result_sel.set_index(result_count++, row);
			i++;
			remaining--;
		}
	}
	return true;
}

#else

struct FSSTPrefilter::State {};

FSSTPrefilter::FSSTPrefilter() : state(make_uniq<State>()) {
}

FSSTPrefilter::~FSSTPrefilter() {
}

bool FSSTPrefilter::Prepare(idx_t, const void *, idx_t, const vector<string> &, idx_t) {
	return false;
}

bool FSSTPrefilter::Filter(const var_binary_view_t &, idx_t, const SelectionVector &, idx_t, SelectionVector &,
                           idx_t &) {
	return false;
}

#endif

} // namespace duckdb
