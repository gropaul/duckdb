#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/fsst.hpp"

namespace duckdb {

VectorFSSTStringBuffer::VectorFSSTStringBuffer(capacity_t capacity, idx_t auxiliary_size)
    : VariableBinaryBuffer(capacity, auxiliary_size) {
	buffer_type = VectorBufferType::FSST_BUFFER;
	vector_type = VectorType::FSST_VECTOR;
}

VectorFSSTStringBuffer::~VectorFSSTStringBuffer() {
}

FSSTEncoder &VectorFSSTStringBuffer::GetEncoder() const {
	if (!fsst_encoder) {
		throw InternalException("FSST vector has no encoder");
	}
	return *fsst_encoder;
}

void VectorFSSTStringBuffer::SetVectorType(VectorType new_vector_type) {
	throw InternalException("SetVectorType not supported for FSST vector");
}

void VectorFSSTStringBuffer::VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(type.InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(vector_type == VectorType::FSST_VECTOR);
}

Value VectorFSSTStringBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (!GetValidityMask().RowIsValid(index)) {
		return Value(type);
	}
	auto str_compressed = GetString(index);
	auto decoder = GetDecoder();
	auto string_val =
	    FSSTPrimitives::DecompressValue(decoder, str_compressed.GetData(), str_compressed.GetSize(), decompress_buffer);
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return Value(std::move(string_val));
	case LogicalTypeId::BLOB:
		return Value::BLOB_RAW(string_val);
	default:
		throw InternalException("Unsupported type for FSST vector GetValue");
	}
}

buffer_ptr<VectorBuffer> VectorFSSTStringBuffer::FlattenSliceInternal(const LogicalType &type,
                                                                      const SelectionVector &sel, idx_t count) const {
	auto result = make_buffer<VectorStringBuffer>(capacity_t(count));

	auto result_data = reinterpret_cast<string_t *>(result->GetData());
	auto &str_allocator = result->GetStringAllocator();
	auto decoder = GetDecoder();
	auto &dst_mask = result->GetValidityMask();
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = sel.get_index(i);
		auto target_idx = i;
		if (!GetValidityMask().RowIsValid(source_idx)) {
			// NULL value
			dst_mask.SetInvalid(target_idx);
			continue;
		}
		auto compressed_string = GetString(source_idx);
		if (compressed_string.GetSize() > 0) {
			result_data[target_idx] = FSSTPrimitives::DecompressValue(
			    decoder, str_allocator, compressed_string.GetData(), compressed_string.GetSize());
		} else {
			// empty string
			result_data[target_idx] = string_t(nullptr, 0);
		}
	}
	result->SetVectorSize(count);
	return result;
}

VectorFSSTStringBuffer &FSSTVector::GetFSSTBuffer(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (vector.GetVectorType() != VectorType::FSST_VECTOR) {
		throw InternalException("FSSTVector::GetFSSTBuffer called on a non-FSST vector");
	}
	if (!vector.GetBufferRef() || vector.Buffer().GetBufferType() != VectorBufferType::FSST_BUFFER) {
		throw InternalException("FSSTVector has a non-FSST buffer");
	}
	return vector.GetBufferRef()->Cast<VectorFSSTStringBuffer>();
}

void *FSSTVector::GetDecoder(const Vector &vector) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetDecoder();
}

vector<unsigned char> &FSSTVector::GetDecompressBuffer(const Vector &vector) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetDecompressBuffer();
}

var_binary_t FSSTVector::GetCompressedString(const Vector &vector, idx_t index) {
	return GetFSSTBuffer(vector).GetVarBinary(index);
}

const var_binary_t *FSSTVector::GetCompressedStrings(const Vector &vector) {
	return GetFSSTBuffer(vector).GetVarBinaries();
}

string FSSTVector::CompressValue(const Vector &vector, const char *input, idx_t input_len) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetEncoder().Compress(input, input_len);
}

void FSSTVector::Create(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder, shared_ptr<FSSTEncoder> encoder,
                        const idx_t string_block_limit, idx_t capacity, idx_t auxiliary_size) {
	vector.SetBuffer(make_buffer<VectorFSSTStringBuffer>(capacity_t(capacity), auxiliary_size));
	auto &fsst_string_buffer = vector.BufferMutable().Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.AddDecoder(duckdb_fsst_decoder, std::move(encoder), string_block_limit);
}

} // namespace duckdb
