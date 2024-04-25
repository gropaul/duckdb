#include "duckdb/execution/operator/projection/physical_fact_expand.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

using SingleScanStructure = PhysicalFactExpand::SingleScanStructure;
using CombinedScanStructure = PhysicalFactExpand::CombinedScanStructure;

class FactExpandState : public OperatorState {
public:
	explicit FactExpandState(ExecutionContext &context) {
	}

	optional_ptr<TupleDataCollection> data_collection;
	// There can be multiple scan structures if we have to fact vectors at the same time
	unique_ptr<CombinedScanStructure> scan_structure;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		// context.thread.profiler.Flush(op, executor, "projection", 0);
	}

	TupleDataCollection *FindDataCollectionInChildren(const unique_ptr<PhysicalOperator> &op) const {

		if (op->type == PhysicalOperatorType::HASH_JOIN) {
			auto physical_hash_join_op = reinterpret_cast<PhysicalHashJoin *>(op.get());
			return physical_hash_join_op->GetHTDataCollection();
		}

		for (auto &child : op->children) {
			auto child_data_collection = FindDataCollectionInChildren(child);
			if (child_data_collection) {
				return child_data_collection;
			}
		}

		return nullptr;
	}
	TupleDataCollection &GetDataCollection(const vector<unique_ptr<PhysicalOperator>> &children) {
		if (!this->data_collection) {
			for (auto &child : children) {
				this->data_collection = FindDataCollectionInChildren(child); // assuming this function returns a pointer
				if (this->data_collection) {
					break;
				}
			}
			// throw an exception if we still don't have a data collection
			if (!this->data_collection) {
				throw InternalException("Could not find data collection in fact expand");
			}
		}

		return *this->data_collection; // Dereferencing the pointer to return a reference
	}
};

SingleScanStructure::SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p)
    : current_pointers_v(LogicalType::POINTER), pointer_offset(pointer_offset_p),
      original_pointers_v(LogicalType::POINTER) {

	UnifiedVectorFormat pointers_v_unified;
	pointers_v.ToUnifiedFormat(count, pointers_v_unified);

	auto current_pointers = FlatVector::GetData<data_ptr_t>(current_pointers_v);
	auto start_pointers = FlatVector::GetData<data_ptr_t>(original_pointers_v);

	auto pointers = UnifiedVectorFormat::GetData<data_ptr_t>(pointers_v_unified);

	for (idx_t row_index = 0; row_index < count; row_index++) {
		auto uvf_index = pointers_v_unified.sel->get_index(row_index);
		current_pointers[row_index] = pointers[uvf_index];
		start_pointers[row_index] = pointers[uvf_index];
	}
}

void SingleScanStructure::AdvancePointers(duckdb::SelectionVector &sel, duckdb::idx_t &count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->current_pointers_v);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx] + pointer_offset);
		if (ptrs[idx]) {
			sel.set_index(new_count++, idx);
		}
	}
	count = new_count;
}

void SingleScanStructure::GetCurrentActivePointers(const idx_t original_count, SelectionVector &sel, idx_t &count) {
	// now for all the pointers, we move on to the next set of pointers
	count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->current_pointers_v);
	for (idx_t i = 0; i < original_count; i++) {
		if (ptrs[i]) {
			sel.set_index(count++, i);
		}
	}
}

void SingleScanStructure::ResetPointers(const SelectionVector &sel, const idx_t &count) {
	// reset the pointers to the start
	auto current_pointers = FlatVector::GetData<data_ptr_t>(current_pointers_v);
	auto original_pointers = FlatVector::GetData<data_ptr_t>(original_pointers_v);

	for (idx_t i = 0; i < count; i++) {
		idx_t row_index = sel.get_index(i);
		current_pointers[row_index] = original_pointers[row_index];
	}
}

CombinedScanStructure::CombinedScanStructure(DataChunk &input, const TupleDataCollection &data_collection_p)
    : sel(STANDARD_VECTOR_SIZE), original_count(input.size()), data_collection(data_collection_p) {

	// set the selection to the whole input chunk
	const idx_t pointer_offset = data_collection.GetLayout().GetOffsets().back();
	idx_t current_result_vector_idx = 0;

	// initialize the selection vector
	SetSelection(*FlatVector::IncrementalSelectionVector(), input.size());

	// iterate over the vectors in the input chunk
	for (column_t input_col_idx = 0; input_col_idx < input.ColumnCount(); input_col_idx++) {

		Vector &input_v = input.data[input_col_idx];

		if (input_v.GetType().id() == LogicalTypeId::FACT_POINTER) {

			is_fact_vector.push_back(true);
			fact_vector_count++;

			// get column mapping for this fact vector
			LogicalType fact_ptr_type = input_v.GetType();
			auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(fact_ptr_type.AuxInfo());
			vector<LogicalType> flat_types = type_info->flat_types;

			vector<column_t> mapping_of_types = vector<column_t>(flat_types.size());

			for (column_t expand_col_idx = 0; expand_col_idx < flat_types.size(); expand_col_idx++) {
				mapping_of_types[expand_col_idx] = current_result_vector_idx;
				current_result_vector_idx++;
			}

			this->fact_column_mappings.push_back(mapping_of_types);

			// initialize single scan structure for this vector
			unique_ptr<SingleScanStructure> single_scan_structure =
			    make_uniq<SingleScanStructure>(input_v, input.size(), pointer_offset);
			this->scan_structures.push_back(std::move(single_scan_structure));

		} else {
			is_fact_vector.push_back(false);
			this->flat_column_mappings.push_back(current_result_vector_idx);
			current_result_vector_idx++;
		}
	}

	fact_vector_count = this->fact_column_mappings.size();
}

void CombinedScanStructure::SetSelection(const SelectionVector &new_sel, const idx_t &new_count) {
	// reset the pointers to the start

	for (idx_t i = 0; i < new_count; i++) {
		sel.set_index(i, new_sel.get_index(i));
	}

	count = new_count;
}
void CombinedScanStructure::AdvancePointers() {
	// Always advance the scan structure the most to the left. If it is exhausted, advance the
	// next scan structure and reset the one before
	idx_t current_vector_idx = 0;

	while (true) {

		//  Advance the current,  updates the internal sel_vector and count to only point at the results where we have
		//  elements
		auto &scan_structure = this->scan_structures[current_vector_idx];
		scan_structure->AdvancePointers(this->sel, this->count);

		// if there is nothing found by the current scan structure
		if (this->count == 0) {

			// we are at the final vector and therefore done
			if (current_vector_idx == fact_vector_count -1) {
				break;
			}

			// we have to increment the next one and reset the selection
			// reset this structure to the current selection of the previous structure
			auto &next_scan_structure = this->scan_structures[current_vector_idx + 1];

			// set the current selection to the active pointers of the next scan structure
			next_scan_structure->GetCurrentActivePointers(this->original_count, this->sel, this->count);
			// reset the current scan structure to the selection of the next scan structure
			scan_structure->ResetPointers(this->sel, this->count);

			current_vector_idx++;


		}
		// our current structure has still some pointers that need to be processed.
		else {
			break;
		}
	}
}

void CombinedScanStructure::Gather(DataChunk &input, DataChunk &result) {

	column_t fact_column_idx = 0;
	column_t flat_column_idx = 0;

	// We have to copy the sel vector for the slicing as it will change in advance pointers and the DataChunks
	// have still a reference to the original sel vector
	SelectionVector slice_sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < this->count; i++) {
		slice_sel.set_index(i, this->sel.get_index(i));
	}

	result.SetCardinality(this->count);

	for (column_t input_column_idx = 0; input_column_idx < input.ColumnCount(); input_column_idx++) {

		Vector &input_v = input.data[input_column_idx];

		if (this->is_fact_vector[input_column_idx]) {

			// get the pointer vector from the corresponding scan structure
			Vector &pointers_v = this->scan_structures[fact_column_idx]->current_pointers_v;

			vector<column_t> column_mappings = this->fact_column_mappings[fact_column_idx];
			fact_column_idx++;

			for (column_t expand_col_idx = 0; expand_col_idx < column_mappings.size(); expand_col_idx++) {
				column_t result_column_idx = column_mappings[expand_col_idx];

				Vector &result_v = result.data[result_column_idx];

				LogicalType fact_ptr_type = input_v.GetType();
				auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(fact_ptr_type.AuxInfo());
				vector<LogicalType> flat_types = type_info->flat_types;
				D_ASSERT(result_v.GetType() == flat_types[expand_col_idx]);

				data_collection.Gather(pointers_v, slice_sel, this->count, expand_col_idx, result_v, slice_sel,
				                       nullptr);
			}
		}
		// flat columns only need to be referenced
		else {
			column_t result_column_idx = this->flat_column_mappings[flat_column_idx];
			flat_column_idx++;

			result.data[result_column_idx].Reference(input_v);
			result.data[result_column_idx].Slice(slice_sel, this->count);
		}
	}
}

void CombinedScanStructure::Next(DataChunk &input, DataChunk &result) {
	Gather(input, result);
	AdvancePointers();
}

bool CombinedScanStructure::PointersExhausted() const {
	return count == 0;
}

PhysicalFactExpand::PhysicalFactExpand(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::FACT_EXPAND, std::move(types), estimated_cardinality) {
}

OperatorResultType PhysicalFactExpand::Execute(ExecutionContext &context, DataChunk &input, DataChunk &result,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FactExpandState>();

	// Make sure we have a data collection
	const auto &data_collection = state.GetDataCollection(this->children);

	if (state.scan_structure) {
		// still have elements remaining (i.e. we got >STANDARD_VECTOR_SIZE elements in the previous probe)
		state.scan_structure->Next(input, result);
		if (!state.scan_structure->PointersExhausted() || result.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.scan_structure = make_uniq<CombinedScanStructure>(input, data_collection);
	state.scan_structure->Next(input, result);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

string PhysicalFactExpand::ParamsToString() const {
	string extra_info;
	return extra_info;
}

unique_ptr<OperatorState> PhysicalFactExpand::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FactExpandState>(context);
}

} // namespace duckdb