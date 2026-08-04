#include "catch.hpp"
#include "duckdb/storage/compression/fsst/fsst_mandatory_chain.hpp"
#include "fsst.h"

#include <cstring>

using namespace duckdb;

namespace {

duckdb_fsst_decoder_t MakeDecoder(const vector<string> &symbol_strings) {
	duckdb_fsst_decoder_t decoder;
	memset(&decoder, 0, sizeof(decoder));
	for (idx_t code = 0; code < symbol_strings.size(); code++) {
		const auto &sym = symbol_strings[code];
		REQUIRE(!sym.empty());
		REQUIRE(sym.size() <= 8);
		decoder.len[code] = static_cast<unsigned char>(sym.size());
		uint64_t value = 0;
		memcpy(&value, sym.data(), sym.size());
		decoder.symbol[code] = value;
	}
	return decoder;
}

vector<uint8_t> Chain(const vector<string> &symbol_strings, const string &needle) {
	auto decoder = MakeDecoder(symbol_strings);
	return FSSTMandatoryChain(&decoder, needle.data(), needle.size());
}

} // namespace

TEST_CASE("FSST mandatory chain", "[fsst]") {
	// unique tiling: the whole path is the chain
	REQUIRE(Chain({"a", "b"}, "ab") == vector<uint8_t> {0, 1});

	// google-like: fixed chain g o o g l, then the exit fans out over er/en/e
	REQUIRE(Chain({"g", "o", "l", "er", "en", "e"}, "google") == vector<uint8_t> {0, 1, 1, 0, 2});

	// entry split: matches can start after 'og' (inside 'oogle') or after 'go' (inside 'ogle');
	// the chain starts at the merge node 'ogle'
	REQUIRE(Chain({"og", "go", "o", "le", "g"}, "google") == vector<uint8_t> {0, 3});

	// a symbol extending past the needle end ('green ') accepts directly from START,
	// bypassing every interior node: no chain
	REQUIRE(Chain({"ing", "pring", "gr", " gre", "green ", "r", "e", "en"}, "green").empty());

	// needle strictly inside one symbol: that single code is the chain
	REQUIRE(Chain({"xgoogleZ"}, "google") == vector<uint8_t> {0});

	// repetitive prefix: 'aa' overlaps the needle start in two ways, both entries valid,
	// and the two tilings share no hop: no chain
	REQUIRE(Chain({"aa", "ab", "b"}, "aaab").empty());

	// 'q' cannot be tiled by any symbol: no chain
	REQUIRE(Chain({"g", "a"}, "gq").empty());

	// empty needle
	REQUIRE(Chain({"a"}, "").empty());
}
