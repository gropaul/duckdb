

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"

#include <duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp>

namespace duckdb {

class PhysicalFactorizedPreAggregateState : public OperatorState {
public:
	explicit PhysicalFactorizedPreAggregateState(const ExecutionContext &context,
	                                             const vector<unique_ptr<Expression>> &aggregates,
	                                             const vector<LogicalType> &factor_types,
	                                             TupleDataCollection *data_collection)
	    : data_collection(data_collection) {

		vector<LogicalType> aggregate_types;
		for (auto &aggr : aggregates) {
			auto &aggr_expr = aggr->Cast<BoundAggregateExpression>();
			aggregate_types.push_back(aggr_expr.return_type);
		}

		factor_vector.Initialize(context.client, factor_types);
		global_aggregate_state =make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context.client), aggregates);
		local_aggregate_state = make_uniq<LocalUngroupedAggregateState>(*global_aggregate_state);

		aggregate_count = aggregates.size();
	}

	idx_t aggregate_count;
	DataChunk factor_vector;
	TupleDataCollection *data_collection;

	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

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
	auto state = make_uniq<PhysicalFactorizedPreAggregateState>(context, aggregates,  factor_types, data_collection);
	return std::move(state);
}

OperatorResultType PhysicalFactorizedPreAggregate::Execute(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &output, GlobalOperatorState &gstate,
                                                           OperatorState &state_p) const {

	auto &state = state_p.Cast<PhysicalFactorizedPreAggregateState>();
	idx_t column_count = input.ColumnCount();
	auto &pointers_v = input.data[column_count -1];

	// get the data collection
	auto &data_collection = state.data_collection;

	const idx_t fact_columns_count = state.factor_vector.ColumnCount();
	vector<column_t> column_ids;
	vector<unique_ptr<Vector>> caches;
	for (column_t col_idx = 0; col_idx < fact_columns_count; col_idx++) {
		column_ids.push_back(col_idx);
		caches.push_back(nullptr);
	}
	state.factor_vector.SetCardinality(input.size());
	data_collection->Gather(pointers_v, *FlatVector::IncrementalSelectionVector(), input.size(), column_ids, state.factor_vector, *FlatVector::IncrementalSelectionVector(), caches);

	for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_count; aggr_idx ++) {
		state.local_aggregate_state->Sink(state.factor_vector, 0, aggr_idx);
	}

	state.global_aggregate_state->Combine(*state.local_aggregate_state);
	state.global_aggregate_state->Finalize(output,0);

	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
