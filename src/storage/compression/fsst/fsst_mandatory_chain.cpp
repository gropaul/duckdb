#include "duckdb/storage/compression/fsst/fsst_mandatory_chain.hpp"

#include "fsst.h"

#include <cstring>

namespace duckdb {

// The needle automaton over FSST codes. Node i (0 <= i <= needle_size) means the first
// i bytes of the needle have been matched; node 0 is START, node needle_size is ACCEPT.
// Every edge consumes at least one needle byte, so edges strictly increase the node index
// and the graph is a DAG. Assumes greedy longest-match encoding (see FSSTEncoder) and does
// not model escape sequences: an unencodable needle yields an empty chain, not a rejection.

namespace {

struct FSSTSymbol {
	unsigned char bytes[8];
	uint8_t length; //! symbol byte-length (1-8), 0 for unused codes
};

struct ChainEdge {
	uint8_t code;
	idx_t dst;
};

//! True if the symbol agrees with needle[offset..] on their common prefix.
bool SymbolMatchesAt(const FSSTSymbol &sym, const unsigned char *needle, idx_t needle_size, idx_t offset) {
	const idx_t remaining = needle_size - offset;
	const idx_t common = sym.length < remaining ? sym.length : remaining;
	return memcmp(sym.bytes, needle + offset, common) == 0;
}

//! Add the edges leaving node offset: every matching symbol that extends past the needle
//! end (a possible match end) plus the single longest symbol that fits fully inside the
//! remaining bytes (the greedy continuation - no shorter symbol can ever be emitted).
void AddTilingEdges(vector<vector<ChainEdge>> &edges, const FSSTSymbol *symbols, const unsigned char *needle,
                    idx_t needle_size, idx_t offset) {
	const idx_t remaining = needle_size - offset;
	idx_t best_fit_len = 0;
	uint8_t best_fit_code = 0;
	for (uint16_t code = 0; code < 255; code++) {
		const auto &sym = symbols[code];
		if (sym.length == 0 || !SymbolMatchesAt(sym, needle, needle_size, offset)) {
			continue;
		}
		if (sym.length > remaining) {
			edges[offset].push_back({static_cast<uint8_t>(code), needle_size});
		} else if (sym.length > best_fit_len) {
			best_fit_len = sym.length;
			best_fit_code = static_cast<uint8_t>(code);
		}
	}
	if (best_fit_len > 0) {
		edges[offset].push_back({best_fit_code, offset + best_fit_len});
	}
}

//! Add the START edges beyond the symbol-boundary ones: matches beginning inside the tail
//! of a preceding symbol (proper suffix of a symbol equals a needle prefix) and matches
//! lying strictly inside a single symbol.
void AddStartEdges(vector<vector<ChainEdge>> &edges, const FSSTSymbol *symbols, const unsigned char *needle,
                   idx_t needle_size) {
	for (uint16_t code = 0; code < 255; code++) {
		const auto &sym = symbols[code];
		for (idx_t start = 1; start < sym.length; start++) {
			const idx_t suffix_len = sym.length - start;
			if (suffix_len <= needle_size && memcmp(sym.bytes + start, needle, suffix_len) == 0) {
				edges[0].push_back({static_cast<uint8_t>(code), suffix_len});
			}
		}
		// strict interior containment; prefix/suffix containment is covered above and by the boundary edges
		for (idx_t pos = 1; pos + needle_size < sym.length; pos++) {
			if (memcmp(sym.bytes + pos, needle, needle_size) == 0) {
				edges[0].push_back({static_cast<uint8_t>(code), needle_size});
				break;
			}
		}
	}
}

//! True if ACCEPT is reachable from START without visiting node avoid.
//! Edges strictly increase the node index, so one ascending sweep suffices.
bool ReachesAcceptAvoiding(const vector<vector<ChainEdge>> &edges, idx_t needle_size, idx_t avoid) {
	vector<bool> reached(needle_size + 1, false);
	reached[0] = true;
	for (idx_t node = 0; node < needle_size; node++) {
		if (!reached[node] || node == avoid) {
			continue;
		}
		for (const auto &edge : edges[node]) {
			reached[edge.dst] = true;
		}
	}
	return reached[needle_size];
}

} // namespace

vector<uint8_t> FSSTMandatoryChain(void *duckdb_fsst_decoder, const char *needle_p, idx_t needle_size) {
	if (!duckdb_fsst_decoder || needle_size == 0) {
		return {};
	}
	const auto &decoder = *static_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	const auto needle = reinterpret_cast<const unsigned char *>(needle_p);

	// extract the symbol table; symbol[] is little-endian, so memory order is the byte sequence
	FSSTSymbol symbols[255];
	for (uint16_t code = 0; code < 255; code++) {
		const idx_t length = decoder.len[code];
		if (length < 1 || length > 8) {
			symbols[code].length = 0;
			continue;
		}
		symbols[code].length = static_cast<uint8_t>(length);
		memcpy(symbols[code].bytes, &decoder.symbol[code], 8);
	}

	// build the automaton: boundary-start edges are the tiling edges of node 0
	vector<vector<ChainEdge>> edges(needle_size + 1);
	for (idx_t node = 0; node < needle_size; node++) {
		AddTilingEdges(edges, symbols, needle, needle_size, node);
	}
	AddStartEdges(edges, symbols, needle, needle_size);

	// prune edges into dead ends (nodes that cannot reach ACCEPT), back to front
	vector<bool> alive(needle_size + 1, false);
	alive[needle_size] = true;
	for (idx_t node = needle_size; node-- > 0;) {
		auto &out = edges[node];
		idx_t kept = 0;
		for (const auto &edge : out) {
			if (alive[edge.dst]) {
				out[kept++] = edge;
			}
		}
		out.resize(kept);
		alive[node] = kept > 0;
	}
	if (!alive[0]) {
		// the needle cannot be tiled with symbols at all
		return {};
	}

	// mandatory nodes: removing them disconnects START from ACCEPT
	vector<idx_t> mandatory;
	mandatory.push_back(0);
	for (idx_t node = 1; node < needle_size; node++) {
		if (!ReachesAcceptAvoiding(edges, needle_size, node)) {
			mandatory.push_back(node);
		}
	}
	mandatory.push_back(needle_size);

	// a hop between consecutive mandatory nodes contributes a fixed code iff every edge
	// out of the node leads directly to the next mandatory node under one single code;
	// the longest run of such hops is the chain
	vector<uint8_t> best;
	vector<uint8_t> current;
	for (idx_t i = 0; i + 1 < mandatory.size(); i++) {
		const auto &out = edges[mandatory[i]];
		bool single_code = !out.empty();
		for (const auto &edge : out) {
			if (edge.dst != mandatory[i + 1] || edge.code != out[0].code) {
				single_code = false;
				break;
			}
		}
		if (single_code) {
			current.push_back(out[0].code);
			continue;
		}
		if (current.size() > best.size()) {
			best = current;
		}
		current.clear();
	}
	if (current.size() > best.size()) {
		best = std::move(current);
	}
	return best;
}

} // namespace duckdb
