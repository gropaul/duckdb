if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(duckdb_fsst_like STATIC FsstWrapper.cpp)

target_include_directories(duckdb_fsst_like PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(duckdb_fsst_like PROPERTIES EXPORT_NAME duckdb_fsst_like)

install(TARGETS duckdb_fsst_like
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_fsst_like)
