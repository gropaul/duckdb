//
// Created by Paul on 08/02/2025.
//

#ifndef ROW_LAYOUT_H
#define ROW_LAYOUT_H
#include <iostream>
#include <utility>

#include "duckdb.hpp"
#include "memory_manager.hpp"
#include "scatter.hpp"


namespace duckdb {
    // 4mb partition size
    constexpr idx_t PARTITION_SIZE = 4 * 1024 * 1024;

    //! Returns the size of the data type in bytes
    static idx_t GetSize(const duckdb::LogicalType &type) {
        const auto size = GetTypeIdSize(type.InternalType());
        return size;
    }

    struct RowLayoutFormat {
        explicit RowLayoutFormat(const std::vector<LogicalType> &types) : types(types) {
            size = 0;
            for (const auto &type: types) {
                offsets.push_back(size);
                size += GetSize(type);
            }
        }

        std::vector<idx_t> offsets;
        const std::vector<LogicalType> &types;
        idx_t size;
    };

    class RowLayoutPartition {
    public:

        idx_t radix;
        idx_t row_count;

        const RowLayoutFormat &format;

        //! In bytes
        idx_t current_write_offset;
        data_ptr_t data;

        const std::vector<scatter_function_t> &scatter_functions;
        const std::vector<gather_function_t> &gather_functions;
        MemoryManager &memory_manager;

        explicit RowLayoutPartition(idx_t radix,
                                    const std::vector<scatter_function_t> &scatter_functions,
                                    const std::vector<gather_function_t> &gather_functions,
                                    const RowLayoutFormat &format, MemoryManager &memory_manager)
            : radix(radix), row_count(0), current_write_offset(0), format(format),
              scatter_functions(scatter_functions), gather_functions(gather_functions), memory_manager(memory_manager) {
            data = memory_manager.allocate(PARTITION_SIZE);
        }

        bool CanFit(const DataChunk &chunk) const {
            auto chunk_count = chunk.size();
            auto chunk_size = chunk_count * format.size;
            return current_write_offset + chunk_size <= PARTITION_SIZE;
        }

        bool Sink(const DataChunk &chunk, const Vector &hashes_v, const SelectionVector &sel, const idx_t count) {
            if (!CanFit(chunk)) {
                return false;
            }

            const idx_t row_width = format.size;

            for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
                auto &vector = i == chunk.ColumnCount() - 1 ? hashes_v : chunk.data[i];
                data_ptr_t target = data + current_write_offset + format.offsets[i];
                auto &scatter_function = scatter_functions[i];
                scatter_function(vector, sel, count, row_width, target);
            }

            const auto written_bytes = row_width * count;
            current_write_offset += written_bytes;
            row_count += count;

            return true;
        }

        void Free() const {
            memory_manager.deallocate(data);
        }

        void Print() const {
            std::cerr << "Partition " << radix << " has " << row_count << " rows" << '\n';
            column_t col_count = format.types.size();
            for (column_t col_idx = 0; col_idx < col_count; col_idx++) {
                std::cerr << "Column " << col_idx << ": ";
                auto &gather_function = gather_functions[col_idx];
                Vector target(format.types[col_idx], row_count);
                data_ptr_t start = data + format.offsets[col_idx];
                gather_function(target, *FlatVector::IncrementalSelectionVector(), row_count, format.size, start);
                target.Print(row_count);
            }
        }
    };

    class RowLayout {
    public:
        explicit RowLayout(const std::vector<LogicalType> &types, std::vector<column_t> key_columns, uint8_t partition_bits,
                           MemoryManager &memory_manager)
            : format(types), memory_manager(memory_manager), hash_v(LogicalType::HASH),
              key_columns(std::move(key_columns)), partition_bits(partition_bits), partition_bit_mask((1 << partition_bits) - 1) {

            // the last type is the hash
            D_ASSERT(types[types.size() - 1] == LogicalType::HASH);

            for (auto &type: types) {
                scatter_functions.push_back(GetScatterFunction(type));
                gather_functions.push_back(GetGatherFunction(type));
            }

            for (idx_t radix = 0; radix < (1 << partition_bits); radix++) {
                partition_copy_count.emplace_back(0);
                partitions_copy_sel.emplace_back(STANDARD_VECTOR_SIZE);
                partitions.emplace_back(radix, scatter_functions, gather_functions, format, memory_manager);
            }
        }

        idx_t partition_bits;
        idx_t partition_bit_mask;

        std::vector<column_t> key_columns;
        RowLayoutFormat format;
        std::vector<RowLayoutPartition> partitions;
        std::vector<scatter_function_t> scatter_functions;
        std::vector<gather_function_t> gather_functions;

        Vector hash_v;
        std::vector<uint64_t> partition_copy_count;
        std::vector<SelectionVector> partitions_copy_sel;

        MemoryManager &memory_manager;

    public:
        void Append(DataChunk &chunk) {
            const idx_t count = chunk.size();

            // create the hash based on the key columns
            const idx_t keys_count = key_columns.size();
            auto &key = chunk.data[key_columns[0]];
            VectorOperations::Hash(key, hash_v, count);

            for (idx_t i = 1; i < keys_count; i++) {
                auto &combined_key = chunk.data[key_columns[i]];
                VectorOperations::CombineHash(hash_v, combined_key, count);
            }

            const auto hashes = FlatVector::GetData<hash_t>(hash_v);
            for (idx_t i = 0; i < count; i++) {
                auto hash = hashes[i];
                auto partition_idx = hash & partition_bit_mask;
                auto &copy_sel = partitions_copy_sel[partition_idx];
                auto &copy_count = partition_copy_count[partition_idx];
                copy_sel.set_index(copy_count, i);
                copy_count++;
            }

            // sink the data into the partitions and reset the copy count
            for (idx_t i = 0; i < (1 << partition_bits); i++) {
                partitions[i].Sink(chunk, hash_v, partitions_copy_sel[i], partition_copy_count[i]);
                partition_copy_count[i] = 0;
            }
        }

        void Free() const {
            for (auto &partition: partitions) {
                partition.Free();
            }
        }

        void Print() const {
            for (const auto &partition: partitions) {
                partition.Print();
            }
        }
    };
}


#endif //ROW_LAYOUT_H
