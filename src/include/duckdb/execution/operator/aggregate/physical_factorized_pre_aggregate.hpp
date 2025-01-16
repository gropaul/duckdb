//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class FactorScanStructure {

	Vector pointers_v;
	idx_t count;
	SelectionVector pointers_sel;

	// the source collection of the factorized vectors
	TupleDataCollection *source_collection;

	explicit FactorScanStructure(TupleDataCollection *source_collection)
	    : pointers_v(LogicalType::POINTER), count(0), pointers_sel(STANDARD_VECTOR_SIZE), source_collection(source_collection) {

	}

	void Initialize(Vector &pointers, const SelectionVector &sel, idx_t count);
	void Next(DataChunk &result);
	void AdvancePointers(const SelectionVector &sel, const idx_t sel_count);
	bool PointersExhausted() const;
};

class PhysicalFactorizedPreAggregateState : public OperatorState {
public:
	explicit PhysicalFactorizedPreAggregateState(const ExecutionContext &context,
	                                             const vector<unique_ptr<Expression>> &aggregates,
	                                             const vector<LogicalType> &factor_types,
	                                             TupleDataCollection *source_collection)
	    : aggr_data_addresses_v(LogicalType::POINTER),
	      source_collection(source_collection) {

		/* Infrastructure for retrieving factorized vectors */

		factor_vector.Initialize(context.client, factor_types);

		/* Infrastructure for computing the aggregates */

		vector<BoundAggregateExpression *> bindings;
		for (auto &aggr : aggregates) {
			D_ASSERT(aggr->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE && aggr->IsAggregate());
			bindings.push_back(&aggr->Cast<BoundAggregateExpression>());
		}

		aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);
		aggr_data_layout.Initialize(aggregate_objects);

		// we need to compute the aggregate per incoming row, -> perfect-hashtable with VECTOR_SIZE entries
		const auto aggr_row_width = aggr_data_layout.GetRowWidth();
		aggr_owned_data = make_unsafe_uniq_array_uninitialized<data_t>(aggr_row_width * STANDARD_VECTOR_SIZE);
		aggr_data = aggr_owned_data.get();

		auto aggr_data_addresses = FlatVector::GetData<uintptr_t>(aggr_data_addresses_v);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			aggr_data_addresses[i] = uintptr_t(aggr_data) + (aggr_row_width * i);
		}

		RowOperations::InitializeStates(aggr_data_layout, aggr_data_addresses_v,
										*FlatVector::IncrementalSelectionVector(), STANDARD_VECTOR_SIZE);
		aggregate_allocator = make_uniq<ArenaAllocator>(Allocator::Get(context.client));

		/* Infrastructure for caching the aggregates */

		vector<LogicalType> aggregate_return_types;
		for (auto &aggr : aggregates) {
			auto &aggr_expr = aggr->Cast<BoundAggregateExpression>();
			aggregate_return_types.push_back(aggr_expr.return_type);
		}

		auto &buffer_manager = BufferManager::GetBufferManager(context.client);
		TupleDataLayout aggregate_cache_layout;
		aggregate_cache_layout.Initialize(aggregate_return_types);
		aggregate_cache_collection = make_uniq<TupleDataCollection>(buffer_manager, aggregate_cache_layout);
	}

	TupleDataLayout aggr_data_layout;
	data_ptr_t aggr_data;
	unsafe_unique_array<data_t> aggr_owned_data;
	Vector aggr_data_addresses_v;
	//! The active arena allocator used by the aggregates for their internal state
	unique_ptr<ArenaAllocator> aggregate_allocator;
	//! The aggregates to be computed
	vector<AggregateObject> aggregate_objects;

	unique_ptr<TupleDataCollection> aggregate_cache_collection;


public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

class PhysicalFactorizedPreAggregate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FACTORIZED_PRE_AGGREGATE;

public:
	PhysicalFactorizedPreAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                               vector<LogicalType> factor_types, idx_t estimated_cardinality);

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	vector<unique_ptr<Expression>> aggregates;
	vector<LogicalType> factor_types;

	bool ParallelOperator() const override {
		return true;
	}

};
} // namespace duckdb