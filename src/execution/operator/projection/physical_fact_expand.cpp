#include "duckdb/execution/operator/projection/physical_fact_expand.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <utility>

namespace duckdb {

SingleScanStructure::SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p,
                                         TupleDataCollection &data_collection_p)
    : current_pointers_v(LogicalType::POINTER), pointer_offset(pointer_offset_p),
      original_pointers_v(LogicalType::POINTER), data_collection(data_collection_p) {

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

CombinedScanStructure::CombinedScanStructure(DataChunk &input, const vector<FactExpandCondition> &conditions,
                                             FactExpandState &state, const PhysicalOperator *op)
    : fact_vector_count(0), sel(STANDARD_VECTOR_SIZE), original_count(input.size()), conditions(conditions) {

	idx_t current_result_vector_idx = 0;

	// initialize the selection vector
	SetSelection(*FlatVector::IncrementalSelectionVector(), input.size());

	// iterate over the vectors in the input chunk
	for (column_t input_col_idx = 0; input_col_idx < input.ColumnCount(); input_col_idx++) {

		Vector &input_v = input.data[input_col_idx];

		if (input_v.GetType().id() == LogicalTypeId::FACT_POINTER) {

			// get column mapping for this fact vector
			LogicalType fact_ptr_type = input_v.GetType();
			auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(fact_ptr_type.AuxInfo());
			auto emitter_id = type_info->emitter_id;

			is_fact_vector.push_back(true);
			emitter_ids.push_back(emitter_id);

			vector<LogicalType> flat_types = type_info->flat_types;
			vector<column_t> mapping_of_types = vector<column_t>(flat_types.size());

			for (column_t expand_col_idx = 0; expand_col_idx < flat_types.size(); expand_col_idx++) {
				mapping_of_types[expand_col_idx] = current_result_vector_idx;
				current_result_vector_idx++;
			}

			this->fact_column_mappings.push_back(mapping_of_types);

			auto &fact_data_collection = state.GetDataCollection(op, fact_vector_count, emitter_id);
			const idx_t pointer_offset = fact_data_collection.GetLayout().GetOffsets().back();

			// initialize single scan structure for this vector
			unique_ptr<SingleScanStructure> single_scan_structure =
			    make_uniq<SingleScanStructure>(input_v, input.size(), pointer_offset, fact_data_collection);
			this->scan_structures.push_back(std::move(single_scan_structure));

			fact_vector_count++;

		} else {
			is_fact_vector.push_back(false);
			this->flat_column_mappings.push_back(current_result_vector_idx);
			current_result_vector_idx++;
		}
	}
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
			if (current_vector_idx == fact_vector_count - 1) {
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

void CombinedScanStructure::Gather(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher) {

	column_t fact_column_idx = 0;
	column_t flat_column_idx = 0;

	// We have to copy the sel vector for the slicing as it will change in advance pointers and the DataChunks
	// have still a reference to the original sel vector
	idx_t slice_count = this->count;
	SelectionVector slice_sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < slice_count; i++) {
		slice_sel.set_index(i, this->sel.get_index(i));
	}

	// match the fitting columns
	if (matcher) {

		column_t lhs_fact_index = matcher->lhs_fact_index;
		column_t rhs_fact_index = matcher->rhs_fact_index;

		Vector &lhs_fact_pointers = this->scan_structures[lhs_fact_index]->current_pointers_v;
		Vector &rhs_fact_pointers = this->scan_structures[rhs_fact_index]->current_pointers_v;

		slice_count = matcher->FactorizedMatch(input, slice_sel, slice_count, lhs_fact_pointers, rhs_fact_pointers);
	} else {
		D_ASSERT(this->conditions.size() == 0);
	}

	for (column_t input_column_idx = 0; input_column_idx < input.ColumnCount(); input_column_idx++) {

		Vector &input_v = input.data[input_column_idx];

		if (this->is_fact_vector[input_column_idx]) {

			D_ASSERT(input_v.GetType().id() == LogicalTypeId::FACT_POINTER);

			auto &scan_structure = this->scan_structures[fact_column_idx];

			// get the pointer vector from the corresponding scan structure
			Vector &pointers_v = scan_structure->current_pointers_v;

			vector<column_t> column_mappings = this->fact_column_mappings[fact_column_idx];
			fact_column_idx++;

			for (column_t expand_col_idx = 0; expand_col_idx < column_mappings.size(); expand_col_idx++) {
				column_t result_column_idx = column_mappings[expand_col_idx];

				Vector &result_v = result.data[result_column_idx];

				LogicalType fact_ptr_type = input_v.GetType();
				auto type_info = reinterpret_cast<const FactPointerTypeInfo *>(fact_ptr_type.AuxInfo());
				vector<LogicalType> flat_types = type_info->flat_types;
				D_ASSERT(result_v.GetType() == flat_types[expand_col_idx]);

				scan_structure->data_collection.Gather(pointers_v, slice_sel, slice_count, expand_col_idx + 1, result_v,
				                                       slice_sel, nullptr);
			}
		}
		// flat columns only need to be referenced
		else {

			D_ASSERT(input_v.GetType().id() != LogicalTypeId::FACT_POINTER);

			column_t result_column_idx = this->flat_column_mappings[flat_column_idx];
			flat_column_idx++;
			result.data[result_column_idx].Reference(input_v);
		}
	}

	result.Slice(slice_sel, slice_count);
}

void CombinedScanStructure::Next(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher) {
	Gather(input, result, matcher);
	AdvancePointers();
}

bool CombinedScanStructure::PointersExhausted() const {
	return count == 0;
}

void FactExpandState::InitializeMatcher(const duckdb::DataChunk &input, const PhysicalFactExpand *op,
                                        const vector<FactExpandCondition> &conditions_p) {
	for (auto &condition : conditions_p) {

		column_t lhs_fact_column = condition.lhs_binding.fact_column_chunk_index;
		LogicalType lhs_fact_type = input.data[lhs_fact_column].GetType();
		D_ASSERT(lhs_fact_type.id() == LogicalTypeId::FACT_POINTER);
		auto lhs_type_info = reinterpret_cast<const FactPointerTypeInfo *>(lhs_fact_type.AuxInfo());
		auto lhs_emitter_id = lhs_type_info->emitter_id;
		auto &lhs_data_collection = this->GetDataCollection(op, lhs_fact_column, lhs_emitter_id);

		column_t rhs_fact_column = condition.rhs_binding.fact_column_chunk_index;
		LogicalType rhs_fact_type = input.data[rhs_fact_column].GetType();
		D_ASSERT(rhs_fact_type.id() == LogicalTypeId::FACT_POINTER);
		auto rhs_type_info = reinterpret_cast<const FactPointerTypeInfo *>(rhs_fact_type.AuxInfo());
		auto rhs_emitter_id = rhs_type_info->emitter_id;
		auto &rhs_data_collection = this->GetDataCollection(op, rhs_fact_column, rhs_emitter_id);

		this->matcher = unique_ptr<FactorizedRowMatcher>(
		    new FactorizedRowMatcher(condition.lhs_binding.fact_column_index, condition.rhs_binding.fact_column_index,
		                             lhs_data_collection, rhs_data_collection, conditions_p));
	}
}

PhysicalFactExpand::PhysicalFactExpand(vector<LogicalType> types, idx_t estimated_cardinality,
                                       vector<FactExpandCondition> conditions)
    : PhysicalOperator(PhysicalOperatorType::FACT_EXPAND, std::move(types), estimated_cardinality),
      conditions(std::move(conditions)) {
}

OperatorResultType PhysicalFactExpand::Execute(ExecutionContext &context, DataChunk &input, DataChunk &result,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FactExpandState>();

	if (state.matcher == nullptr && this->conditions.size() > 0) {
		state.InitializeMatcher(input, this, this->conditions);
	}

	if (state.scan_structure) {
		// still have elements remaining (i.e. we got >STANDARD_VECTOR_SIZE elements in the previous probe)
		state.scan_structure->Next(input, result, state.matcher);
		if (!state.scan_structure->PointersExhausted() || result.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.scan_structure = make_uniq<CombinedScanStructure>(input, this->conditions, state, this);
	state.scan_structure->Next(input, result, state.matcher);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

string PhysicalFactExpand::ParamsToString() const {
	string extra_info;
	for (auto &condition : conditions) {
		extra_info += "\n";
		auto expr_string = condition.lhs_binding.alias + " " + ExpressionTypeToOperator(condition.comparison) + " " +
		                   condition.rhs_binding.alias;
		extra_info += expr_string;
	}
	return extra_info;
}

unique_ptr<OperatorState> PhysicalFactExpand::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FactExpandState>(context);
}

} // namespace duckdb