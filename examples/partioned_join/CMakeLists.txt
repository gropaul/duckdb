cmake_minimum_required(VERSION 2.8.12...3.29)
project(HTBench)

set(CMAKE_CXX_STANDARD 11)

include_directories(../../src/include)
link_directories(../../build/release/src)

include_directories(src)
add_executable(HTBench main.cpp
        src/materialization/row_layout.cpp
        src/materialization/row_layout.hpp
        src/materialization/memory_manager.cpp
        src/materialization/memory_manager.hpp
        src/materialization/scatter.hpp)
target_link_libraries(HTBench duckdb)
