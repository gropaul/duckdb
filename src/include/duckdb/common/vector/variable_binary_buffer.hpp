//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/variable_binary_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! A non-owning view of a value: raw bytes + length, pointing into the byte buffer (no copy, never inlined).
struct var_binary_t {
	const char *ptr;
	idx_t length;

	//! Wrap a string_t as a view. Note: for inlined string_t the pointer is into the string_t itself,
	//! so the source must outlive the view.
	static var_binary_t FromString(const string_t &str) {
		return {str.GetData(), str.GetSize()};
	}

	bool operator==(const var_binary_t &other) const {
		return length == other.length && memcmp(ptr, other.ptr, length) == 0;
	}
};

//! Stores variable-length binary values as a contiguous byte buffer plus a precomputed var_binary_t array.
//! var_binaries[i] is the {ptr, length} view of value i, pointing directly into the byte buffer, so reads are a
//! single array load with no per-lookup arithmetic. Values are stored REVERSED in the byte buffer (value 0 at the
//! end, the last value at the front) because that is how they arrive from the dictionary block. Pre-sized at construction.
class VariableBinaryBuffer : public VectorBuffer {
public:
	//! element_count: number of values this buffer will hold (also the vector size - pre-sized, fully filled)
	//! auxiliary_size: total number of payload bytes across all values
	VariableBinaryBuffer(idx_t element_count, idx_t auxiliary_size,
	                     Allocator &allocator = Allocator::DefaultAllocator())
	    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::VARIABLE_BINARY_BUFFER, count_t(element_count)),
	      var_binary_data(allocator.Allocate(sizeof(var_binary_t) * (element_count == 0 ? 1 : element_count))),
	      byte_data(allocator.Allocate(auxiliary_size == 0 ? 1 : auxiliary_size)), capacity(element_count) {
		var_binaries = reinterpret_cast<var_binary_t *>(var_binary_data.get());
		byte_data_ptr = byte_data.get();
		validity.Resize(element_count);
	}

public:
	ValidityMask &GetValidityMask() override {
		return validity;
	}
	const ValidityMask &GetValidityMask() const override {
		return validity;
	}
	idx_t Capacity() const override {
		return capacity;
	}
	//! Payload bytes; not exposed via GetData() so FlatVector::GetData<string_t> cannot misread it
	data_ptr_t GetBytes() {
		return byte_data_ptr;
	}
	const_data_ptr_t GetBytes() const {
		return byte_data_ptr;
	}
	//! The precomputed views; populators fill this array, callers can grab it once and index directly
	var_binary_t *GetVarBinaries() {
		return var_binaries;
	}
	const var_binary_t *GetVarBinaries() const {
		return var_binaries;
	}
	//! Raw bytes + length of value index (points into the byte buffer; safe for any length)
	var_binary_t GetVarBinary(idx_t index) const {
		return var_binaries[index];
	}
	//! View of value index as a string_t into the byte buffer (no copy)
	string_t GetString(idx_t index) const {
		auto &view = var_binaries[index];
		return string_t(view.ptr, UnsafeNumericCast<uint32_t>(view.length));
	}

private:
	ValidityMask validity;
	//! precomputed {ptr, length} views, one per value, pointing into byte_data
	AllocatedData var_binary_data;
	var_binary_t *var_binaries;

	//! contiguous payload bytes
	AllocatedData byte_data;
	data_ptr_t byte_data_ptr;
	//! number of values this buffer holds
	idx_t capacity;
};

} // namespace duckdb
