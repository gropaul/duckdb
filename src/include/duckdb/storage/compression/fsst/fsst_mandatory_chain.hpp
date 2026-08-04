//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/fsst/fsst_mandatory_chain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Computes the longest sequence of FSST codes that appears contiguously in the compressed
//! form of every string containing needle, given the symbol table of the decoder.
//! The result can be searched for directly in the compressed data (e.g. as a memmem pattern).
//! An empty result means no fixed chain exists for this needle and symbol table.
vector<uint8_t> FSSTMandatoryChain(void *duckdb_fsst_decoder, const char *needle, idx_t needle_size);

} // namespace duckdb
