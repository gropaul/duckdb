//
// Created by Paul on 09/02/2025.
//

#ifndef SCATTER_H
#define SCATTER_H

#endif //SCATTER_H

#include "duckdb.hpp"

// generic function to scatter a chunk of data into a partition
namespace duckdb {
    typedef void (*scatter_function_t)(const Vector &source, const SelectionVector &sel, const idx_t count,
                                       const idx_t jump_offset, data_ptr_t target);

    typedef void (*gather_function_t)(Vector &target, const SelectionVector &sel, const idx_t count,
                                      const idx_t jump_offset, data_ptr_t source);

    template<typename DATA_TYPE>
    void Scatter(const Vector &source, const SelectionVector &sel, const idx_t count,
                 const idx_t jump_offset, data_ptr_t partition_data) {
        auto source_data = FlatVector::GetData<DATA_TYPE>(source);
        for (idx_t i = 0; i < count; i++) {
            auto source_idx = sel.get_index(i);
            auto source_value = source_data[source_idx];
            Store<DATA_TYPE>(source_value, partition_data + jump_offset * i);
        }
    }

    template<typename DATA_TYPE>
    void Gather(Vector &target, const SelectionVector &sel, const idx_t count,
                const idx_t jump_offset, data_ptr_t source) {
        auto target_data = FlatVector::GetData<DATA_TYPE>(target);
        for (idx_t i = 0; i < count; i++) {
            auto source_idx = sel.get_index(i);
            auto source_value = Load<DATA_TYPE>(source + jump_offset * i);
            target_data[source_idx] = source_value;
        }
    }

    static scatter_function_t GetScatterFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return Scatter<int64_t>;
            case LogicalTypeId::UBIGINT:
                return Scatter<uint64_t>;
            case LogicalTypeId::INTEGER:
                return Scatter<int32_t>;
            case LogicalTypeId::UINTEGER:
                return Scatter<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return Scatter<int16_t>;
            case LogicalTypeId::USMALLINT:
                return Scatter<uint16_t>;
            case LogicalTypeId::TINYINT:
                return Scatter<int8_t>;
            case LogicalTypeId::UTINYINT:
                return Scatter<uint8_t>;
            case LogicalTypeId::FLOAT:
                return Scatter<float>;
            case LogicalTypeId::DOUBLE:
                return Scatter<double>;
            default:
                throw NotImplementedException("Scatter function not implemented for type: " + type.ToString());
        }
    }

    static gather_function_t GetGatherFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return Gather<int64_t>;
            case LogicalTypeId::UBIGINT:
                return Gather<uint64_t>;
            case LogicalTypeId::INTEGER:
                return Gather<int32_t>;
            case LogicalTypeId::UINTEGER:
                return Gather<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return Gather<int16_t>;
            case LogicalTypeId::USMALLINT:
                return Gather<uint16_t>;
            case LogicalTypeId::TINYINT:
                return Gather<int8_t>;
            case LogicalTypeId::UTINYINT:
                return Gather<uint8_t>;
            case LogicalTypeId::FLOAT:
                return Gather<float>;
            case LogicalTypeId::DOUBLE:
                return Gather<double>;
            default:
                throw NotImplementedException("Gather function not implemented for type");
        }
    }
};
