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

//! Stores variable-length binary values as a contiguous byte buffer plus a descending int32 offsets array.
//! offsets holds physical byte positions (capacity + 1 entries): offsets[0] == total, offsets[capacity] == 0, and
//! value i occupies byte_data[offsets[i + 1] : offsets[i]] (start = offsets[i+1], length = offsets[i] - offsets[i+1]).
//! Values are stored REVERSED in the byte buffer (value 0 at the end, the last value at the front) because that is
//! how they arrive from the dictionary block. Pre-sized at construction.
class VariableBinaryBuffer : public VectorBuffer {
public:
	//! element_count: number of values this buffer will hold (also the vector size - pre-sized, fully filled)
	//! auxiliary_size: total number of payload bytes across all values
	VariableBinaryBuffer(idx_t element_count, idx_t auxiliary_size,
	                     Allocator &allocator = Allocator::DefaultAllocator())
	    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::VARIABLE_BINARY_BUFFER, count_t(element_count)),
	      offset_data(allocator.Allocate(sizeof(int32_t) * (element_count + 1))),
	      byte_data(allocator.Allocate(auxiliary_size == 0 ? 1 : auxiliary_size)), capacity(element_count) {
		offsets = reinterpret_cast<int32_t *>(offset_data.get());
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
	//! Descending physical offsets; populators fill this, callers can grab it once and index directly
	int32_t *GetOffsets() {
		return offsets;
	}
	const int32_t *GetOffsets() const {
		return offsets;
	}
	//! Raw bytes + length of value index (points into the byte buffer; safe for any length)
	var_binary_t GetVarBinary(idx_t index) const {
		const int32_t start = offsets[index + 1];
		const int32_t end = offsets[index];
		return {const_char_ptr_cast(byte_data_ptr) + start, idx_t(end - start)};
	}
	//! View of value index as a string_t into the byte buffer (no copy)
	string_t GetString(idx_t index) const {
		auto view = GetVarBinary(index);
		return string_t(view.ptr, UnsafeNumericCast<uint32_t>(view.length));
	}

	//! Total payload bytes currently held (reverse layout: offsets[0] is the top of the byte buffer)
	idx_t ByteSize() const {
		return capacity == 0 ? 0 : idx_t(offsets[0]);
	}

	//! Grow to hold added_count more values / added_bytes more payload, preserving existing contents.
	//! The new (later-scanned) block goes at the FRONT of the byte buffer, so existing bytes shift UP by added_bytes
	//! and existing offsets shift up by the same amount. This keeps the whole buffer one contiguous reverse-packed
	//! region, so the shared boundary stays valid across blocks. Leaves offset slots [old capacity + 1 ..] and byte
	//! region [0 .. added_bytes) for the caller to fill.
	void Grow(idx_t added_count, idx_t added_bytes) {
		auto &alloc = *byte_data.GetAllocator();
		const idx_t old_capacity = capacity;
		const idx_t old_bytes = ByteSize();
		const idx_t new_capacity = old_capacity + added_count;
		const idx_t new_bytes = old_bytes + added_bytes;

		AllocatedData new_offset_data = alloc.Allocate(sizeof(int32_t) * (new_capacity + 1));
		auto new_offsets = reinterpret_cast<int32_t *>(new_offset_data.get());
		for (idx_t i = 0; i <= old_capacity; i++) {
			new_offsets[i] = offsets[i] + UnsafeNumericCast<int32_t>(added_bytes);
		}

		AllocatedData new_byte_data = alloc.Allocate(new_bytes == 0 ? 1 : new_bytes);
		memcpy(new_byte_data.get() + added_bytes, byte_data_ptr, old_bytes);

		offset_data = std::move(new_offset_data);
		byte_data = std::move(new_byte_data);
		offsets = new_offsets;
		byte_data_ptr = byte_data.get();
		capacity = new_capacity;
		validity.Resize(new_capacity);
		SetVectorSizeOnly(new_capacity);
	}

private:
	ValidityMask validity;
	//! descending physical byte positions, capacity + 1 entries; offsets[0] == total, offsets[capacity] == 0
	AllocatedData offset_data;
	int32_t *offsets;

	//! contiguous payload bytes
	AllocatedData byte_data;
	data_ptr_t byte_data_ptr;
	//! number of values this buffer holds
	idx_t capacity;
};

} // namespace duckdb
