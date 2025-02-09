//
// Created by Paul on 08/02/2025.
//

#ifndef MEMORY_MANAGER_H
#define MEMORY_MANAGER_H
#include <unordered_map>
#include "duckdb.hpp"

namespace duckdb {
    class MemoryManager {
    public:
        // Allocate a block of memory and return a pointer to the block.
        data_ptr_t allocate(size_t size);

        // Deallocate a previously allocated block of memory.
        void deallocate(data_ptr_t ptr);

        // Get the current number of active allocations.
        size_t getActiveAllocations() const;

        // Destructor to clean up any remaining allocations.
        ~MemoryManager();

    private:
        // Map to store active allocations with their sizes.
        std::unordered_map<void *, size_t> allocations;
    };
}
#endif  // MEMORY_MANAGER_H
