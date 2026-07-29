add_library_unity(duckdb_storage_compression_fsst OBJECT
                  fsst_mandatory_chain.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression_fsst>
    PARENT_SCOPE)
