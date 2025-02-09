//
// Created by Paul on 08/02/2025.
//


#include "memory_manager.hpp"
#include <iostream>
#include <cstdlib>  // for malloc and free

namespace duckdb {
    data_ptr_t MemoryManager::allocate(std::size_t size) {
        void *ptr = std::malloc(size);
        if (!ptr) {
            throw std::bad_alloc();
        }
        allocations[ptr] = size;
        return static_cast<data_ptr_t>(ptr);
    }

    void MemoryManager::deallocate(data_ptr_t ptr) {
        auto it = allocations.find(static_cast<void*>(ptr));
        if (it != allocations.end()) {
            std::free(ptr);
            allocations.erase(it);
        } else {
            throw std::runtime_error("Attempt to deallocate an unknown or already freed pointer.");
        }
    }

    std::size_t MemoryManager::getActiveAllocations() const {
        return allocations.size();
    }

    MemoryManager::~MemoryManager() {
        for (const auto& pair : allocations) {
            std::cerr << "Warning: Memory leak detected. Address: " << pair.first
                      << ", Size: " << pair.second << " bytes\n";
            std::free(pair.first);
        }
        allocations.clear();
    }
}
