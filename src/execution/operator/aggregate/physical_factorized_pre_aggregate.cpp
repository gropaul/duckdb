

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"

#include <duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp>

namespace duckdb {


PhysicalFactorizedPreAggregate::PhysicalFactorizedPreAggregate(vector<LogicalType> types,
                                                               vector<unique_ptr<Expression>> expressions,
                                                               vector<LogicalType> factor_types,
                                                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::FACTORIZED_PRE_AGGREGATE, std::move(types), estimated_cardinality),
      aggregates(std::move(expressions)), factor_types(std::move(factor_types)) {
}

static TupleDataCollection *FindDataCollectionInOp(const PhysicalOperator *op, const idx_t &emitter_id) {

	if (op->type == PhysicalOperatorType::HASH_JOIN) {
		auto physical_hash_join_op = reinterpret_cast<const PhysicalHashJoin *>(op);
		auto collection = physical_hash_join_op->GetHTDataCollection(0);
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

unique_ptr<OperatorState> PhysicalFactorizedPreAggregate::GetOperatorState(ExecutionContext &context) const {

	auto data_collection = FindDataCollectionInOp(children[0].get(), 0);
	auto state = make_uniq<PhysicalFactorizedPreAggregateState>(context, aggregates, factor_types,data_collection);
	return std::move(state);
}


OperatorResultType PhysicalFactorizedPreAggregate::Execute(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &output, GlobalOperatorState &gstate,
                                                           OperatorState &state_p) const {

	auto &state = state_p.Cast<PhysicalFactorizedPreAggregateState>();
	idx_t column_count = input.ColumnCount();
	auto &pointers_v = input.data[column_count - 1];

	// get the data collection
	auto &data_collection = state.source_collection;

	const idx_t fact_columns_count = state.factor_vector.ColumnCount();
	vector<column_t> column_ids;
	vector<unique_ptr<Vector>> caches;
	for (column_t col_idx = 0; col_idx < fact_columns_count; col_idx++) {
		column_ids.push_back(col_idx);
		caches.push_back(nullptr);
	}
	state.factor_vector.SetCardinality(input.size());
	data_collection->Gather(pointers_v, *FlatVector::IncrementalSelectionVector(), input.size(), column_ids,
	                        state.factor_vector, *FlatVector::IncrementalSelectionVector(), caches);


	RowOperationsState row_state(*state.aggregate_allocator);
	// Copy the addresses
	Vector addresses_copy(LogicalType::POINTER);
	VectorOperations::Copy(state.aggr_data_addresses_v, addresses_copy, STANDARD_VECTOR_SIZE, 0, 0);

	idx_t payload_idx = 0;
	for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_objects.size(); aggr_idx++) {
		auto &aggregate = state.aggregate_objects[aggr_idx];
		RowOperations::UpdateStates(row_state, aggregate, addresses_copy, state.factor_vector, payload_idx,
		                            state.factor_vector.size());
		// todo: we need to reset the vector again
		VectorOperations::AddInPlace(addresses_copy, NumericCast<int64_t>(aggregate.payload_size), state.factor_vector.size());
	}

	// finalize the aggregates
	output.SetCardinality(state.factor_vector.size());
	RowOperations::FinalizeStates(row_state, state.aggr_data_layout, state.aggr_data_addresses_v, output, 0);

	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
