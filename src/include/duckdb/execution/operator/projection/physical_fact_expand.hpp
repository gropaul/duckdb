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

	vector<FactorizedRowMatcher> matchers;

	// data source
	const TupleDataCollection &data_collection;

	explicit SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p,
	                             TupleDataCollection &data_collection);

	void AdvancePointers(duckdb::SelectionVector &found_sel, duckdb::idx_t &found_count);

	void GetCurrentActivePointers(const idx_t original_count, duckdb::SelectionVector &sel, duckdb::idx_t &count);

	void ResetPointers(const SelectionVector &sel, const idx_t &count);
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
	// which of the factorized vectors currently is stepped
	column_t vector_to_step_idx;
	// how many fact vectors there are to be flattened
	column_t fact_vector_count;

	// emitter ids for each fact vector
	vector<idx_t> emitter_ids;

	// used to only gather data where we still have pointers in the chain
	idx_t count;
	SelectionVector sel;

	// how many pointers we have in the chain in the beginning
	idx_t original_count;

	// the conditions for the scan
	vector<FactExpandCondition> conditions;

	void Next(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher);
	void Gather(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher);
	void SetSelection(const SelectionVector &original_sel, const idx_t &original_count);

	//! Are pointer chains all pointing to NULL?
	bool PointersExhausted() const;

private:
	void AdvancePointers();
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

	static TupleDataCollection *FindDataCollectionInOp(const PhysicalOperator *op, const idx_t &emitter_id) {

		if (op->type == PhysicalOperatorType::HASH_JOIN) {
			auto physical_hash_join_op = reinterpret_cast<const PhysicalHashJoin *>(op);
			auto collection = physical_hash_join_op->GetHTDataCollection(emitter_id);
			// can be null e.g. if wrong emitter id
			if (collection != nullptr) {
				return collection;
			}
		}

		for (auto &child : op->children) {
			auto child_data_collection = FindDataCollectionInOp(child.get(), emitter_id);
			if (child_data_collection) {
				return child_data_collection;
			}
		}

		return nullptr;
	}
	void InitializeMatcher(const duckdb::DataChunk &input, const PhysicalFactExpand *op,
	                       const vector<FactExpandCondition> &conditions_p);

private:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		// context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

} // namespace duckdb
