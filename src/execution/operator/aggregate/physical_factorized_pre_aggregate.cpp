

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"

#include <duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp>

namespace duckdb {

void FactorScanStructure::Initialize(Vector &initial_pointers_v, idx_t count) {

	// transform pointers to uniform vector format and initialize the internal pointers as flat vector
	UnifiedVectorFormat initial_pointers_v_uni;
	initial_pointers_v.ToUnifiedFormat(count, initial_pointers_v_uni);
	auto initial_pointers = UnifiedVectorFormat::GetData<data_ptr_t>(initial_pointers_v_uni);

	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_v);

	for (idx_t idx = 0; idx < count; idx++) {
		auto unified_idx = initial_pointers_v_uni.sel->get_index(idx);
		pointers[idx] = initial_pointers[unified_idx];
	}

	this->count = count;

	// reset the selection vector
	for (idx_t i = 0; i < count; i++) {
		pointers_sel.set_index(i, i);
	}

}

void FactorScanStructure::AdvancePointers(const SelectionVector &sel, const idx_t sel_count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto pointers = FlatVector::GetData<data_ptr_t>(this->pointers_v);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		pointers[idx] = cast_uint64_to_pointer(Load<uint64_t>(pointers[idx] + pointer_offset));
		if (pointers[idx]) {
			this->pointers_sel.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void FactorScanStructure::AdvancePointers() {
	AdvancePointers(this->pointers_sel, this->count);
}

bool FactorScanStructure::PointersExhausted() const {
	return count == 0;
}

void FactorScanStructure::Next(DataChunk &result) {
	const idx_t fact_columns_count = fact_column_offsets.size();

	vector<unique_ptr<Vector>> caches;
	for (column_t col_idx = 0; col_idx < fact_columns_count; col_idx++) {
		caches.push_back(nullptr);
	}

	result.SetCardinality(count);
	source_collection->Gather(pointers_v, pointers_sel, count, fact_column_offsets, result,
	                          *FlatVector::IncrementalSelectionVector(), caches);
}


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

	vector<idx_t> factor_column_offsets;
	for (column_t col_idx = 0; col_idx < factor_types.size(); col_idx++) {
		factor_column_offsets.push_back(0);
	}

	auto state = make_uniq<PhysicalFactorizedPreAggregateState>(context, aggregates, factor_types,
	                                                            factor_column_offsets, data_collection);
	return std::move(state);
}

OperatorResultType PhysicalFactorizedPreAggregate::Execute(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &output, GlobalOperatorState &gstate,
                                                           OperatorState &state_p) const {
	auto &state = state_p.Cast<PhysicalFactorizedPreAggregateState>();
	auto &scan_structure = state.factor_scan_structure;

	idx_t column_count = input.ColumnCount();
	auto &pointers_v = input.data[column_count - 1];
	scan_structure.Initialize(pointers_v, input.size());

	RowOperationsState row_state(*state.aggregate_allocator);

	// Copy the addresses
	Vector aggregate_addresses_v(LogicalType::POINTER);
	auto aggregate_addresses = FlatVector::GetData<data_ptr_t>(aggregate_addresses_v);
	auto aggregate_addresses_initial = FlatVector::GetData<data_ptr_t>(state.aggr_data_addresses_v);
	DataChunk &payload = state.factor_data;

	do {

		// get the next set of pointers
		scan_structure.Next(payload);
		idx_t payload_count = payload.size();
		idx_t payload_idx = 0;

		// update the aggregate pointers according to the scann structure selection, create a dense array of pointers
		for (idx_t idx = 0; idx < scan_structure.GetCount(); idx++) {
			auto sel_idx = scan_structure.GetPointersSel().get_index(idx);
			aggregate_addresses[idx] = aggregate_addresses_initial[sel_idx];
		}

		// update the aggregates
		for (idx_t aggr_idx = 0; aggr_idx < state.aggregate_objects.size(); aggr_idx++) {
			auto &aggregate = state.aggregate_objects[aggr_idx];
			RowOperations::UpdateStates(row_state, aggregate, aggregate_addresses_v, payload, payload_idx, payload_count);
			VectorOperations::AddInPlace(aggregate_addresses_v, NumericCast<int64_t>(aggregate.payload_size), payload_count);
		}

		scan_structure.AdvancePointers();

	} while (!scan_structure.PointersExhausted());

	// finalize the aggregates
	output.SetCardinality(input.size());
	RowOperations::FinalizeStates(row_state, state.aggr_data_layout, state.aggr_data_addresses_v, output, 0);

	RowOperations::InitializeStates(state.aggr_data_layout, state.aggr_data_addresses_v,
								*FlatVector::IncrementalSelectionVector(), STANDARD_VECTOR_SIZE);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb

