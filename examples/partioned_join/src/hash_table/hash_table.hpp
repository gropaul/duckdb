//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include <iostream>

uint64_t inline next_power_of_two(uint64_t n) {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n++;
    return n;
}


namespace duckdb {
    class HashTable {
    public:


        data_ptr_t ht1_array;
        data_ptr_t ht2_array;

        HashTable(uint64_t number_of_records, MemoryManager &memory_manager) {
            uint64_t capacity = next_power_of_two(2 * number_of_records);

            uint64_t ht1_size = capacity * sizeof(uint8_t);
            uint64_t ht2_size = capacity * sizeof(uint64_t);

            ht1_array = memory_manager.allocate(ht1_size + ht2_size);
            ht2_array = ht1_array + ht1_size;
        }

        void Insert(DataChunk &chunk) {

        }
    };
}



#endif //HASH_TABLE_H
