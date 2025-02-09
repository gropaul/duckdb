#include "duckdb.hpp"
#include "materialization/row_layout.hpp"
#include "src/hash_table/hash_table.hpp"

using namespace duckdb;

uint64_t time(time_point<high_resolution_clock> start, bool print = false) {
    const auto end = high_resolution_clock::now();
    const auto duration = duration_cast<milliseconds>(end - start);
    if (print) {
        std::cout << "Time=" << duration.count() << "ms" << '\n';
    }
    return duration.count();
}

void test_materialization(uint8_t partition_bits) {
    DuckDB db("/Users/paul/micro.duckdb");

    Connection con(db);

    std::vector<column_t> keys = {0};
    auto result = con.Query("SELECT key, hash, CAST(order_c AS INT64) FROM build_with_hash_sorted LIMIT 10;");

    auto next_chunk = result->Fetch();
    if (!next_chunk) {
        throw std::runtime_error("No data");
    }
    auto types = next_chunk->GetTypes();
    types.push_back(LogicalType::HASH);

    // time the start of the build
    auto start = std::chrono::high_resolution_clock::now();

    MemoryManager mm;
    RowLayout layout(types, keys, partition_bits, mm);

    while (next_chunk) {
        layout.Append(*next_chunk);
        next_chunk = result->Fetch();
    }

    std::cout << "Bits=" << (int) partition_bits << ' ';
    time(start, true);
    layout.Print();
    layout.Free();
}

int main() {
    test_materialization(2);
    return 0;
    for (uint8_t i = 0; i < 9; i++) {
        test_materialization(i);
    }
}
