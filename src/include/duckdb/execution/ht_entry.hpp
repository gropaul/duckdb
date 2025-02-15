//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/ht_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! The ht_entry_t struct represents an individual entry within a hash table.
/*!
    This struct is used by the JoinHashTable and AggregateHashTable to store entries within the hash table. It stores
    a pointer to the data and a salt value in a single hash_t and can return or modify the pointer and salt
    individually.
*/
struct ht_entry_t { // NOLINT
public:
	//! Bit 0 is used to mark all hashes in RowLayout entries so that we know when the next pointer of an entry is a
	//! hash and therefore the chain is finished
	static constexpr const hash_t HASK_MARK_MASK = 0x8000000000000000;
	//! Bit 0 is the collision bit, marks that on this entry there was a collision during insertion
	static constexpr const hash_t COLLISION_BIT_MASK = 0x8000000000000000;
	//! Bits 1 to 15 are used as salt for pre-comparison of the key
	static constexpr const hash_t SALT_MASK = 0x7FFF000000000000;
	//! Lower 48 bits are the pointer to the row entry
	static constexpr const hash_t POINTER_MASK = 0x0000FFFFFFFFFFFF;

	explicit inline ht_entry_t(hash_t value_p) noexcept : value(value_p) {
	}

	// Add a default constructor for 32-bit linux test case
	inline ht_entry_t() noexcept : value(0) {
	}

	inline bool IsOccupied() const {
		return value != 0;
	}

	inline bool HasCollision() const {
		return (value & COLLISION_BIT_MASK) != 0;
	}

	inline static void MarkAsCollided(std::atomic<ht_entry_t> &entry) {

		// cast ht_entry_t to hash_t in order to use atomic OR operation
		atomic<hash_t> &entry_value = reinterpret_cast<atomic<hash_t> &>(entry);

		// set the collision using the atomic |= operation
		entry_value |= COLLISION_BIT_MASK;
	}

	// Returns a pointer based on the stored value without checking cell occupancy.
	// This can return a nullptr if the cell is not occupied.
	inline data_ptr_t GetPointerOrNull() const {
		return cast_uint64_to_pointer(value & POINTER_MASK);
	}

	// Returns a pointer based on the stored value if the cell is occupied
	inline data_ptr_t GetPointer() const {
		D_ASSERT(IsOccupied());
		return GetPointerOrNull();
	}

	inline void SetPointer(const data_ptr_t &pointer) {
		// Pointer shouldn't use upper bits
		D_ASSERT((cast_pointer_to_uint64(pointer) & SALT_MASK) == 0);
		// Value should have all 1's in the pointer area
		D_ASSERT((value & POINTER_MASK) == POINTER_MASK);
		// Set upper bits to 1 in pointer so the salt stays intact
		value &= cast_pointer_to_uint64(pointer) | ~POINTER_MASK;
	}

	// Returns the salt, leaves upper salt bits intact, sets other bits to all 1's
	static inline hash_t ExtractSalt(hash_t hash) {
		return hash | ~SALT_MASK;
	}

	// Returns the salt, leaves upper salt bits intact, sets other bits to all 0's
	static inline hash_t ExtractSaltWithNulls(hash_t hash) {
		return hash & SALT_MASK;
	}

	inline hash_t GetSalt() const {
		return ExtractSalt(value);
	}

	inline void SetSalt(const hash_t &salt) {
		// Shouldn't be occupied when we set this
		D_ASSERT(!IsOccupied());
		// Salt should have all 1's in the pointer field
		D_ASSERT((salt & POINTER_MASK) == POINTER_MASK);
		// No need to mask, just put the whole thing there
		value = salt;
	}

	static inline ht_entry_t GetNewEntry(const data_ptr_t &pointer, const hash_t &salt) {
		auto desired = cast_pointer_to_uint64(pointer) | (salt & SALT_MASK);
		return ht_entry_t(desired);
	}

	/// Keeps the salt and the Collision bit intact, but updates the pointer
	static inline ht_entry_t UpdateWithPointer(const ht_entry_t &entry, const data_ptr_t &pointer) {

		// slot must be occupied and have a valid pointer
		D_ASSERT(entry.IsOccupied());
		D_ASSERT(entry.GetPointer() != nullptr);

		// set the pointer bits in entry to zero
		auto value_without_pointer = entry.value & ~POINTER_MASK;

		// now update the bits with the new pointer
		auto desired_value = value_without_pointer | cast_pointer_to_uint64(pointer);
		auto desired = ht_entry_t(desired_value);

		return desired;
	}

	static inline ht_entry_t GetEmptyEntry() {
		return ht_entry_t(0);
	}

protected:
	hash_t value;
};

// uses an AND operation to apply the modulo operation instead of an if condition that could result in a branch
// misprediction
inline void IncrementAndWrap(idx_t &offset, const uint64_t &capacity_mask) {
	++offset &= capacity_mask;
}

} // namespace duckdb
