#include "duckdb/execution/operator/projection/physical_fact_expand.hpp"

#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <utility>

namespace duckdb {

child_list_t<LogicalType> FromLogicalTypes(const vector<LogicalType> &types) {
	child_list_t<LogicalType> result;
	for (idx_t i = 0; i < types.size(); i++) {
		string name = to_string(i);
		std::pair<string, LogicalType> pair = std::make_pair(name, types[i]);
		result.push_back(pair);
	}
	return result;
}

SingleScanStructure::SingleScanStructure(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p,
                                         TupleDataCollection &data_collection_p,
                                         const vector<LogicalType> &flat_types_p)
    : pointer_offset(pointer_offset_p),
      list_vector_v(LogicalType::LIST(LogicalType::STRUCT(FromLogicalTypes(flat_types_p)))), list_state(),
      data_collection(data_collection_p) {

	this->pointer_info = FactVectorPointerInfo(pointers_v, count, pointer_offset);

	ListVector::Reserve(list_vector_v, pointer_info.ptrs.size());

	auto list_data = ListVector::GetData(list_vector_v);
	idx_t list_start_idx = 0;
	for (idx_t i = 0; i < pointer_info.ListsCount(); i++) {

		idx_t list_end_idx = pointer_info.list_end_idx[i];

		list_data[i].offset = list_start_idx;
		list_data[i].length = list_end_idx - list_start_idx;

		list_start_idx = list_end_idx;
	}

	Vector all_pointer_vector(LogicalType::POINTER, pointer_info.PointerCount());
	auto all_pointers = FlatVector::GetData<data_ptr_t>(all_pointer_vector);
	for (idx_t i = 0; i < pointer_info.PointerCount(); i++) {
		all_pointers[i] = pointer_info.ptrs[i];
	}

	auto &entries = this->GetListVectors();
	auto flat_sel = FlatVector::IncrementalSelectionVector();

	for (column_t flat_col_idx = 0; flat_col_idx < flat_types_p.size(); flat_col_idx++) {
		Vector &target_vector = *entries[flat_col_idx].get();
		D_ASSERT(target_vector.GetType() == flat_types_p[flat_col_idx]);

		data_collection.Gather(all_pointer_vector, *flat_sel, pointer_info.PointerCount(), flat_col_idx, target_vector,
		                       *flat_sel, nullptr);
	}
}

idx_t &SingleScanStructure::ListVectorIdx() {
	return list_state.ListVectorIdx();
}

idx_t &SingleScanStructure::GetRowIdx() {
	return list_state.GetRowIdx();
}

void SingleScanStructure::ResetToCurrentListHead() {
	return list_state.ResetToCurrentListHead(pointer_info);
}

bool SingleScanStructure::Increment() {
	return list_state.Increment(pointer_info);
}

CombinedScanStructure::CombinedScanStructure(DataChunk &input, const vector<FactExpandCondition> &conditions,
                                             FactExpandState &state, const PhysicalOperator *op)
    : fact_vector_count(0), original_count(input.size()), conditions(conditions) {
	idx_t current_result_vector_idx = 0;

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
			    make_uniq<SingleScanStructure>(input_v, input.size(), pointer_offset, fact_data_collection, flat_types);
			this->scan_structures.push_back(std::move(single_scan_structure));

			fact_vector_count++;

		} else {
			is_fact_vector.push_back(false);
			this->flat_column_mappings.push_back(current_result_vector_idx);
			current_result_vector_idx++;
		}
	}
}

void CombinedScanStructure::IntersectLists() {
	for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
		auto &info_1 = this->scan_structures[0]->pointer_info;
		auto &info_2 = this->scan_structures[2]->pointer_info;

		idx_t list_1_start = info_1.GetListStart(row_idx);
		idx_t list_1_end = info_1.GetListEnd(row_idx);

		idx_t list_2_start = info_2.GetListStart(row_idx);
		idx_t list_2_end = info_2.GetListEnd(row_idx);
	}
}

ScanSelection CombinedScanStructure::GetScanSelectionSequential(idx_t max_tuples) {

	idx_t tuple_index = 0;

	ScanSelection result(max_tuples, fact_vector_count);

	for (;;) {

		for (column_t fact_vector = 0; fact_vector < fact_vector_count; fact_vector++) {
			auto list_vector_idx = scan_structures[fact_vector]->ListVectorIdx();
			auto &sel_v = result.factor_sel[fact_vector];
			sel_v.set_index(tuple_index, list_vector_idx);

			if (fact_vector == 0) {
				auto &row_idx = scan_structures[fact_vector]->GetRowIdx();
				result.flat_sel.set_index(tuple_index, row_idx);
			} else {
				auto &row_idx = scan_structures[fact_vector]->GetRowIdx();
				auto &base_row_idx = scan_structures[0]->GetRowIdx();
				D_ASSERT(row_idx == base_row_idx);
			}
		}

		idx_t current_fact_vector_idx = 0;
		bool all_done = true;

		// increment. If we are at the end of the list, reset and increment next
		while (current_fact_vector_idx < fact_vector_count) {

			auto &scan_structure = scan_structures[current_fact_vector_idx];

			// increment the current scan structure
			bool list_complete = scan_structure->Increment();

			// if we are at the final vector, and it is done we are done
			if (list_complete) {

				// if at the last and all are complete we are done
				auto list_vector_idx = scan_structure->ListVectorIdx();
				bool done = list_vector_idx == scan_structure->pointer_info.PointerCount();

				all_done = all_done && done;

				bool is_last_vector = current_fact_vector_idx == fact_vector_count - 1;

				if (is_last_vector) {
					if (all_done) {
						result.Finalize(tuple_index + 1, true);
						return result;
					}
				}

				// go to the next fact vector
				current_fact_vector_idx++;

			} else {
				// reset the previous state if we are not in the first
				if (current_fact_vector_idx != 0) {
					scan_structures[current_fact_vector_idx - 1]->ResetToCurrentListHead();
				}
				break;
			}
		}

		tuple_index++;

		if (tuple_index == max_tuples) {
			result.Finalize(max_tuples, false);
			return result;
		}
	}
}

void CombinedScanStructure::SliceResult(DataChunk &input, DataChunk &result, const ScanSelection &selection) const {

	column_t fact_column_idx = 0;
	column_t flat_column_idx = 0;
	result.SetCardinality(selection.count);

	for (column_t input_col_idx = 0; input_col_idx < input.ColumnCount(); input_col_idx++) {
		Vector &input_v = input.data[input_col_idx];
		if (is_fact_vector[input_col_idx]) {
			D_ASSERT(input_v.GetType().id() == LogicalTypeId::FACT_POINTER);

			auto &scan_structure = scan_structures[fact_column_idx];

			auto list_sel = selection.factor_sel[fact_column_idx];
			auto &list_vectors = scan_structure->GetListVectors();
			vector<column_t> column_mappings = this->fact_column_mappings[fact_column_idx];

			for (column_t expand_col_idx = 0; expand_col_idx < column_mappings.size(); expand_col_idx++) {

				column_t result_column_idx = column_mappings[expand_col_idx];
				Vector &result_v = result.data[result_column_idx];
				Vector &list_vector = *list_vectors[expand_col_idx].get();

				// todo: We have a missmatch between which column is which in the sel vector, column_mappings is wrong
				result_v.Slice(list_vector, list_sel, selection.count);
			}

			fact_column_idx++;

		} else {
			D_ASSERT(input_v.GetType().id() != LogicalTypeId::FACT_POINTER);

			column_t result_column_idx = flat_column_mappings[flat_column_idx];
			Vector &result_v = result.data[result_column_idx];

			result_v.Slice(input_v, selection.flat_sel, selection.count);
			flat_column_idx++;
		}
	}
}

bool CombinedScanStructure::Next(DataChunk &input, DataChunk &result, unique_ptr<FactorizedRowMatcher> &matcher) {
	auto scan_selection = GetScanSelectionSequential(STANDARD_VECTOR_SIZE);
	SliceResult(input, result, scan_selection);
	return scan_selection.done;
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

	if (state.matcher == nullptr && !this->conditions.empty()) {
		state.InitializeMatcher(input, this, this->conditions);
	}

	// init a new scan structure if we don't have one
	if (!state.scan_structure) {
		state.scan_structure = make_uniq<CombinedScanStructure>(input, this->conditions, state, this);
	}

	bool complete = state.scan_structure->Next(input, result, state.matcher);
	if (!complete) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
	state.scan_structure = nullptr;
	return OperatorResultType::NEED_MORE_INPUT;
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

FactVectorPointerInfo::FactVectorPointerInfo(Vector &pointers_v, const idx_t &count, const idx_t &pointer_offset_p) {

	// flatten the pointers vector
	pointers_v.Flatten(count);
	auto pointers = FlatVector::GetData<data_ptr_t>(pointers_v);

	idx_t last_list_start = 0;
	for (idx_t row_index = 0; row_index < count; row_index++) {
		data_ptr_t current = pointers[row_index];
		while (current) {
			this->ptrs.push_back(current);
			current = Load<data_ptr_t>(current + pointer_offset_p);
		}

		last_list_start = this->ptrs.size();
		this->list_end_idx.push_back(last_list_start);
	}
}
IncrementResult::IncrementResult(bool list_complete, bool done) : list_complete(list_complete), done(done) {
}
IncrementResult::IncrementResult() : list_complete(false), done(false) {
}
} // namespace duckdb