

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include "duckdb/execution/operator/aggregate/physical_factorized_pre_aggregate.hpp"

#include <duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp>
#include <sys/stat.h>

namespace duckdb {

void FactorScanStructure::Initialize(Vector &initial_pointers_v, SelectionVector &initial_pointers_sel, idx_t count) {

	// transform pointers to uniform vector format and initialize the internal pointers as flat vector
	UnifiedVectorFormat initial_pointers_v_uni;
	initial_pointers_v.ToUnifiedFormat(count, initial_pointers_v_uni);
	auto initial_pointers = UnifiedVectorFormat::GetData<data_ptr_t>(initial_pointers_v_uni);

	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_v);

	for (idx_t idx = 0; idx < count; idx++) {
		auto sel_idx = initial_pointers_sel.get_index(idx);
		auto unified_idx = initial_pointers_v_uni.sel->get_index(sel_idx);

		// todo: (open) maybe we need to use sel_idx here
		pointers[idx] = initial_pointers[unified_idx];
		pointers_sel.set_index(idx, idx);
	}

	this->count = count;

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
	const auto &next_pointer_offset = state.factor_scan_structure.GetNextPointerOffset();

	// check which factors aggregate we need to compute and which to load from cache
	idx_t column_count = input.ColumnCount();
	idx_t row_count = input.size();
	auto &row_locations_factors_v = input.data[column_count - 1];

	// transform pointers to uniform vector format and initialize the internal pointers as flat vector
	UnifiedVectorFormat row_locations_factors_unified;
	row_locations_factors_v.ToUnifiedFormat(row_count, row_locations_factors_unified);
	auto row_locations_factors = UnifiedVectorFormat::GetData<data_ptr_t>(row_locations_factors_unified);
	auto row_locations_load_cache = FlatVector::GetData<data_ptr_t>(state.row_locations_load_cache_v);

	state.compute_aggregates_count = 0;
	state.load_aggregates_count = 0;
	for (idx_t idx = 0; idx < row_count; idx++) {
		auto unified_idx = row_locations_factors_unified.sel->get_index(idx);
		uint64_t next_pointer = Load<uint64_t>(row_locations_factors[unified_idx] + next_pointer_offset);

		bool is_aggregate_pointer = (next_pointer & CACHE_POINTER_MASK) == CACHE_POINTER_MASK;

		if (is_aggregate_pointer) {
			state.load_aggregates_sel.set_index(state.load_aggregates_count, idx);
			row_locations_load_cache[state.load_aggregates_count] = cast_uint64_to_pointer(next_pointer & ~CACHE_POINTER_MASK);
			state.load_aggregates_count += 1;
		} else {
			state.compute_aggregates_sel.set_index(state.compute_aggregates_count, idx);
			state.compute_aggregates_count += 1;
		}

	}

	// initialize the scan structure for factors we need to compute
	scan_structure.Initialize(row_locations_factors_v,  state.compute_aggregates_sel, state.compute_aggregates_count);

	// Copy the addresses
	Vector aggregate_addresses_v(LogicalType::POINTER);
	auto aggregate_addresses = FlatVector::GetData<data_ptr_t>(aggregate_addresses_v);
	auto aggregate_addresses_initial = FlatVector::GetData<data_ptr_t>(state.aggr_data_addresses_v);
	DataChunk &payload = state.factor_data;

	// reset the aggregate states
	RowOperations::InitializeStates(state.aggr_data_layout, state.aggr_data_addresses_v,
							scan_structure.GetPointersSel(), scan_structure.GetCount());
	RowOperationsState row_state(*state.aggregate_allocator);

	// compute the aggregates that are not in the cache
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

	// todo: must be behind if we also have column bindings for the groups
	const idx_t aggr_idx = 0;

	// finalize the aggregates (-> compute the final result)
	state.aggregate_data.SetCardinality(state.compute_aggregates_count);
	RowOperations::FinalizeStates(row_state, state.aggr_data_layout, state.aggr_data_addresses_v, state.aggregate_data, aggr_idx);

	// save aggregates to cache
	state.aggregate_cache_collection->Append(state.aggregate_cache_append_state, state.aggregate_data, state.compute_aggregates_sel, state.compute_aggregates_count);

	// mark cache row pointers with mask and save the cache row pointers to the factor scan structure
	auto &row_locations_cache_v = state.aggregate_cache_append_state.chunk_state.row_locations;
	auto row_locations_cache = FlatVector::GetData<data_ptr_t>(row_locations_cache_v);

	for (idx_t idx = 0; idx < state.compute_aggregates_count; idx++) {

		idx_t sel_idx = state.compute_aggregates_sel.get_index(idx);
		idx_t fact_pointer_unified_idx = row_locations_factors_unified.sel->get_index(sel_idx);
		data_ptr_t fact_pointer = row_locations_factors[fact_pointer_unified_idx];

		data_ptr_t cache_pointer = row_locations_cache[idx];
		D_ASSERT(cache_pointer != nullptr);
		uint64_t cache_pointer_marked = cast_pointer_to_uint64(cache_pointer) | CACHE_POINTER_MASK;

		Store(cache_pointer_marked, fact_pointer + next_pointer_offset);
	}

	// copy the now freshly computed aggregates to the output

	// gather the aggregates from the cache
	vector<unique_ptr<Vector>> caches;
	for (column_t col_idx = 0; col_idx < column_count; col_idx++) {
		caches.push_back(nullptr);
	}

	output.SetCardinality(state.load_aggregates_count);
	state.aggregate_cache_collection->Gather(state.row_locations_load_cache_v, *FlatVector::IncrementalSelectionVector(), state.load_aggregates_count, output, *FlatVector::IncrementalSelectionVector(), caches);

	// increment the &state.compute_aggregates_sel by state.load_aggregates_count
	output.Append(state.aggregate_data, false, nullptr, state.compute_aggregates_count);

	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb

