//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/fsst_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/variable_binary_buffer.hpp"

namespace duckdb {

class FSSTEncoder;

class VectorFSSTStringBuffer : public VariableBinaryBuffer {
public:
	VectorFSSTStringBuffer(capacity_t capacity, idx_t auxiliary_size);
	~VectorFSSTStringBuffer() override;

public:
	void AddDecoder(buffer_ptr<void> &duckdb_fsst_decoder_p, shared_ptr<FSSTEncoder> encoder,
	                const idx_t string_block_limit) {
		duckdb_fsst_decoder = duckdb_fsst_decoder_p;
		fsst_encoder = std::move(encoder);
		decompress_buffer.resize(string_block_limit + 1);
	}
	void *GetDecoder() const {
		return duckdb_fsst_decoder.get();
	}
	//! The encoder shared with all vectors created from the same decoder.
	FSSTEncoder &GetEncoder() const;
	vector<unsigned char> &GetDecompressBuffer() const {
		return decompress_buffer;
	}
	void SetVectorType(VectorType vector_type) override;

public:
	Value GetValue(const LogicalType &type, idx_t index) const override;

protected:
	buffer_ptr<VectorBuffer> FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
	                                              idx_t count) const override;
	void VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;

private:
	buffer_ptr<void> duckdb_fsst_decoder;
	mutable vector<unsigned char> decompress_buffer;
	shared_ptr<FSSTEncoder> fsst_encoder;
};

struct FSSTVector {
	static inline const ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.Buffer().GetValidityMask();
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return vector.BufferMutable().GetValidityMask();
	}
	static inline void SetValidity(Vector &vector, ValidityMask &new_validity) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		auto &validity = vector.BufferMutable().GetValidityMask();
		validity.Initialize(new_validity);
	}
	static inline const string_t *GetCompressedData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return reinterpret_cast<const string_t *>(vector.GetBufferRef()->GetData());
	}
	static inline string_t *GetCompressedData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FSST_VECTOR);
		return reinterpret_cast<string_t *>(vector.BufferMutable().GetData());
	}

	DUCKDB_API static void Create(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder,
	                              shared_ptr<FSSTEncoder> encoder, const idx_t string_block_limit, idx_t capacity,
	                              idx_t auxiliary_size);
	//! Grow an existing FSST vector to hold added_count more values / added_bytes more payload (append across scans)
	DUCKDB_API static void Grow(Vector &vector, idx_t added_count, idx_t added_bytes);
	DUCKDB_API static void *GetDecoder(const Vector &vector);
	DUCKDB_API static vector<unsigned char> &GetDecompressBuffer(const Vector &vector);
	//! Raw compressed bytes + length of value index (points into the byte buffer; no copy)
	DUCKDB_API static var_binary_t GetCompressedString(const Vector &vector, idx_t index);
	//! Base of the compressed byte buffer. Fetch once, then index with GetOffsets to avoid per-value buffer lookups.
	DUCKDB_API static const char *GetBasePointer(const Vector &vector);
	//! Descending physical offsets (capacity + 1 entries); value i = base[offsets[i + 1] : offsets[i]]
	DUCKDB_API static const int32_t *GetOffsets(const Vector &vector);
	//! Compress a string using the vector's symbol table, returning the compressed bytes.
	DUCKDB_API static string CompressValue(const Vector &vector, const char *input, idx_t input_len);

private:
	//! FSSTStorage fills the buffer with compressed bytes + offsets during scan
	friend struct FSSTStorage;
	static VectorFSSTStringBuffer &GetFSSTBuffer(const Vector &vector);
};

} // namespace duckdb
