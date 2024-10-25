//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/row_operations/row_matcher.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/operator/logical_fact_expand.hpp"
#include "duckdb/execution/fact_utils.hpp"

namespace duckdb {
class FactExpandState;
struct CombinedScanStructure;

static ExpressionType MapConditionToFlat(const ExpressionType &expression_type) {
	if (expression_type == ExpressionType::COMPARE_FACT_EQUAL) {
		return ExpressionType::COMPARE_EQUAL;
	}

	throw NotImplementedException("Condition not mappable");
}

// Can match all predicates between two distinct factorized columns
class FactorizedRowMatcher : RowMatcher {

	// the conditions for the match
	vector<FactExpandCondition> conditions;

	vector<column_t> lhs_nested_columns;
	vector<column_t> rhs_nested_columns;

	TupleDataCollection &lhs_collection;
	TupleDataCollection &rhs_collection;

	const TupleDataLayout &rhs_layout;

	TupleDataChunkState lhs_chunk_state;
	TupleDataChunkState rhs_chunk_state;

public:
	// the column to gather (left side of the predilhs_fact_columncates)
	column_t lhs_fact_index;
	// the column to match against (right side of the predicates)
	column_t rhs_fact_index;

	explicit FactorizedRowMatcher(column_t lhs_fact_index_p, column_t rhs_fact_index_p,
	                              TupleDataCollection &lhs_collection, TupleDataCollection &rhs_collection,
	                              const vector<FactExpandCondition> &conditions)
	    : conditions(conditions), lhs_collection(lhs_collection), rhs_collection(rhs_collection),
	      rhs_layout(rhs_collection.GetLayout()), lhs_fact_index(lhs_fact_index_p), rhs_fact_index(rhs_fact_index_p) {

		vector<ExpressionType> predicates;

		// get the flat columns from the conditions
		for (auto &condition : conditions) {
			// the fact columns are the same for all conditions for each matcher
			D_ASSERT(condition.lhs_binding.fact_column_index == lhs_fact_index_p);
			D_ASSERT(condition.rhs_binding.fact_column_index == rhs_fact_index_p);

			// the flat columns are the ones that are compared
			lhs_nested_columns.push_back(condition.lhs_binding.nested_column_index + 1);
			rhs_nested_columns.push_back(condition.rhs_binding.nested_column_index + 1);

			// collect the flat predicates
			ExpressionType flat_comparison = MapConditionToFlat(condition.comparison);
			predicates.push_back(flat_comparison);
		}

		lhs_collection.InitializeChunkState(lhs_chunk_state, lhs_nested_columns);
		rhs_collection.InitializeChunkState(rhs_chunk_state, rhs_nested_columns);

		RowMatcher::Initialize(false, rhs_layout, predicates, rhs_nested_columns);
	}

	idx_t FactorizedMatch(DataChunk &input, SelectionVector &sel, idx_t count, Vector &lhs_fact_pointers,
	                      Vector &rhs_fact_pointers) {
		DataChunk lhs_data;
		lhs_collection.InitializeChunk(lhs_data, lhs_nested_columns); // makes sure DataChunk has the right format
		lhs_data.SetCardinality(count);

		// Gather the lhs as we can only match on the flat columns
		lhs_collection.Gather(lhs_fact_pointers, sel, count, lhs_nested_columns, lhs_data, sel,
		                      lhs_chunk_state.cached_cast_vectors);

		DataChunk rhs_data;
		rhs_collection.InitializeChunk(rhs_data, rhs_nested_columns); // makes sure DataChunk has the right format
		rhs_data.SetCardinality(count);

		// Gather the rhs as we can only match on the flat columns
		rhs_collection.Gather(rhs_fact_pointers, sel, count, rhs_nested_columns, rhs_data, sel,
		                      rhs_chunk_state.cached_cast_vectors);

		// cast the lhs data to the unified format to make it comparable
		TupleDataCollection::ToUnifiedFormatMine(lhs_chunk_state, lhs_data);

		idx_t no_match_count = 0;

		// match the lhs with the rhs
		idx_t match_count = RowMatcher::Match(lhs_data, lhs_chunk_state.vector_data, sel, count, rhs_layout,
		                                      rhs_fact_pointers, nullptr, no_match_count, rhs_nested_columns, true);
		return match_count;
	}
};

struct FactVectorPointerInfo {
	vector<data_ptr_t> ptrs;
	vector<idx_t> list_end_idx;
	FactVectorPointerInfo() = default;
	FactVectorPointerInfo(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset);

	idx_t PointerCount() const {
		return ptrs.size();
	}

	idx_t ListsCount() const {
		return list_end_idx.size();
	}

	idx_t ListEndOffset(idx_t list_idx) const {
		return list_end_idx[list_idx];
	}

	idx_t GetListStart(idx_t row_idx) const {
		if (row_idx == 0) {
			return 0;
		}
		return list_end_idx[row_idx - 1];
	}

	idx_t GetListEnd(idx_t row_idx) const {
		return list_end_idx[row_idx];
	}
};

struct IncrementResult {
	IncrementResult();
	IncrementResult(bool list_complete, bool done);
	// with this element a list is complete
	bool list_complete;
	// with this element all lists are processed
	bool done;
};

struct SingleListState {
	// which list of the vector we are currently in
	idx_t row_idx;
	// the current list position physical overarching list
	idx_t list_vector_idx;

	SingleListState() : row_idx(0), list_vector_idx(0) {
	}

	static bool ListComplete(idx_t list_vector_idx, idx_t row_idx, const FactVectorPointerInfo &pointer_info) {
		idx_t list_end_index = pointer_info.ListEndOffset(row_idx);
		return list_vector_idx == list_end_index;
	}

	static bool Done(idx_t list_vector_idx, const FactVectorPointerInfo &pointer_info) {
		return list_vector_idx == pointer_info.PointerCount() - 1;
	}

	// returns a reference to the index
	idx_t &ListVectorIdx() {
		return list_vector_idx;
	}

	idx_t &GetRowIdx() {
		return row_idx;
	}

	bool Increment(const FactVectorPointerInfo &pointer_info) {
		list_vector_idx++;

		bool list_complete = ListComplete(list_vector_idx, row_idx, pointer_info);

		if (list_complete) {
			row_idx++;
		}

		return list_complete;
	}

	void ResetToCurrentListHead(const FactVectorPointerInfo &pointer_info) {
		// only reset if we are at the head of a list
		D_ASSERT(ListComplete(this->list_vector_idx, row_idx - 1, pointer_info));
		idx_t last_list_start = 0;
		if (row_idx >= 2) {
			last_list_start = pointer_info.ListEndOffset(row_idx - 2);
		}
		row_idx -= 1;
		list_vector_idx = last_list_start;
	}
};

//! Scan structure that can be used to resume scans, as a single probe can
//! return VECTOR_SIZE*CHAIN_LENGTH values. Similar to the one of in the HashTable, but with reduced functionality
struct SingleScanStructure {

	//! Next pointer offset in tuple, also used for the position of the hash, which then gets overwritten by the
	//! pointer
	idx_t pointer_offset;

	vector<FactorizedRowMatcher> matchers;

	FactVectorPointerInfo pointer_info;

	Vector list_vector_v;

	SingleListState list_state;

	// data source
	const TupleDataCollection &data_collection;

	explicit SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p,
	                             TupleDataCollection &data_collection_p, const vector<LogicalType> &flat_types_p);

	// returns a reference to the index
	idx_t &ListVectorIdx();
	idx_t &GetRowIdx();
	void ResetToCurrentListHead();
	bool Increment();

	vector<unique_ptr<Vector>> &GetListVectors() {
		auto &list_vector_entries = ListVector::GetEntry(list_vector_v);
		return StructVector::GetEntries(list_vector_entries);
	}
};

struct ScanSelection {
	SelectionVector flat_sel;
	vector<SelectionVector> factor_sel;
	idx_t count;
	bool done;

	ScanSelection(idx_t vector_size, idx_t fact_vector_count) : flat_sel(vector_size), count(0) {
		factor_sel.resize(fact_vector_count);
		for (idx_t i = 0; i < fact_vector_count; i++) {
			factor_sel[i] = SelectionVector(vector_size);
		}
	}

	void Finalize(idx_t count_p, bool done_p) {
		count = count_p;
		done = done_p;
	}
};

struct CombinedScanStructure {

	explicit CombinedScanStructure(DataChunk &input, const vector<FactExpandCondition> &conditions,
	                               FactExpandState &state, const PhysicalOperator *op);

	// one scan structure for each fact vector in the DataChunk
	vector<unique_ptr<SingleScanStructure>> scan_structures;
	// the column mappings for all flat vectors in the DataChunk (1:1)
	vector<column_t> flat_column_mappings;
	// the column mappings for all fact vectors in the DataChunk (n:n)
	vector<vector<column_t>> fact_column_mappings;
	// has a mapping for whether a column is flat or factorized
	vector<bool> is_fact_vector;

	// how many fact vectors there are to be flattened
	column_t fact_vector_count;

	// emitter ids for each fact vector
	vector<idx_t> emitter_ids;

	// how many pointers we have in the chain in the beginning
	idx_t original_count;

	// the conditions for the scan
	vector<FactExpandCondition> conditions;

	bool Next(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher);
	void SliceResult(DataChunk &input, DataChunk &result, const ScanSelection &selection) const;

private:
	ScanSelection GetScanSelectionSequential(idx_t max_tuples);
};

class PhysicalFactExpand : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FACT_EXPAND;

public:
	PhysicalFactExpand(vector<LogicalType> types, idx_t estimated_cardinality, vector<FactExpandCondition> conditions);

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &result,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;

private:
	// the conditions for the fact expansion
	vector<FactExpandCondition> conditions;
};

class FactExpandState : public OperatorState {
public:
	explicit FactExpandState(ExecutionContext &context) {
	}

	vector<optional_ptr<TupleDataCollection>> data_collections;
	// There can be multiple scan structures if we have to fact vectors at the same time
	unique_ptr<CombinedScanStructure> scan_structure;
	// the matcher for the factorized rows
	unique_ptr<FactorizedRowMatcher> matcher;

	TupleDataCollection &GetDataCollection(const PhysicalOperator *op, const column_t &fact_column,
	                                       const idx_t &emitter_id) {

		if (fact_column >= data_collections.size()) {
			data_collections.resize(fact_column + 1);
		}
		auto &fact_column_collection = this->data_collections[fact_column];

		if (!fact_column_collection) {
			fact_column_collection = FindDataCollectionInOp(op, emitter_id);
			// throw an exception if we still don't have a data collection
			if (!fact_column_collection) {
				throw InternalException("Could not find data collection in fact expand");
			}
		}

		return *fact_column_collection; // Dereferencing the pointer to return a reference
	}

	void InitializeMatcher(const duckdb::DataChunk &input, const PhysicalFactExpand *op,
	                       const vector<FactExpandCondition> &conditions_p);

private:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		// context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

} // namespace duckdb
