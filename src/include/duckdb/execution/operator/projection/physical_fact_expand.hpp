//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class PhysicalFactExpand : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FACT_EXPAND;

public:
	PhysicalFactExpand(vector<LogicalType> types, idx_t estimated_cardinality);

	//! Scan structure that can be used to resume scans, as a single probe can
	//! return VECTOR_SIZE*CHAIN_LENGTH values. Similar to the one of in the HashTable, but with reduced functionality
	struct SingleScanStructure {

		Vector current_pointers_v;

		bool finished;
		//! Next pointer offset in tuple, also used for the position of the hash, which then gets overwritten by the
		//! pointer
		idx_t pointer_offset;

		// when we reset one list to start with the next element of the next list, we also need to reset the selectino
		Vector original_pointers_v;

		explicit SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p);

		void AdvancePointers(duckdb::SelectionVector &found_sel, duckdb::idx_t &found_count);

		void GetCurrentActivePointers(const idx_t original_count, duckdb::SelectionVector &sel, duckdb::idx_t &count);

		void ResetPointers(const SelectionVector &sel, const idx_t &count);
	};

	struct CombinedScanStructure {
		// one scan structure for each fact vector in the DataChunk
		vector<unique_ptr<SingleScanStructure>> scan_structures;
		// the column mappings for all flat vectors in the DataChunk (1:1)
		vector<column_t> flat_column_mappings;
		// the column mappings for all fact vectors in the DataChunk (n:n)
		vector<vector<column_t>> fact_column_mappings;
		// has a mapping for whether a column is flat or factorized
		vector<bool> is_fact_vector;
		// which of the factorized vectors currently is stepped
		column_t vector_to_step_idx;
		// how many fact vectors there are to be flattened
		column_t fact_vector_count;

		// used to only gather data where we still have pointers in the chain
		idx_t count;
		SelectionVector sel;

		// how many pointers we have in the chain in the beginning
		idx_t original_count;

		// data source
		const TupleDataCollection &data_collection;

		explicit CombinedScanStructure(DataChunk &input, const TupleDataCollection &data_collection);
		void Next(DataChunk &input, DataChunk &result);
		void Gather(DataChunk &input, DataChunk &result);
		void SetSelection(const SelectionVector &original_sel, const idx_t &original_count);

		//! Are pointer chains all pointing to NULL?
		bool PointersExhausted() const;

	private:
		void AdvancePointers();
	};

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &result,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;
};

} // namespace duckdb
