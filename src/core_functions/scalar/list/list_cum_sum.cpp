
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"

namespace duckdb {


template <class NUMERIC_TYPE>
static void ExecuteConstantCumSum(Vector &result, Vector &list_vector, const idx_t count, SelectionVector &sel, idx_t &sel_id) {

	const auto list_count = ListVector::GetListSize(list_vector);
	auto &list_child = ListVector::GetEntry(list_vector);

	D_ASSERT(list_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(list_child).CheckAllValid(list_count)) {
		throw InvalidInputException("list_cum_sum: argument can not contain NULL values");
	}

	auto list_data = FlatVector::GetData<NUMERIC_TYPE>(list_child);
	auto result_data = FlatVector::GetData<list_entry_t>(result);

	NUMERIC_TYPE sum = 0;
	vector<NUMERIC_TYPE> cum_sum_vector(list_count);

	for (idx_t i = 0; i < list_count; i++) {
		auto value = list_data[i];
		sum += value;
		cum_sum_vector[i] = sum;
	}

	auto cum_sum = make_uniq<Vector>(LogicalType::LIST(LogicalType::INTEGER));
	for (auto val : cum_sum_vector) {
		Value value_to_insert = val;
		ListVector::PushBack(*cum_sum, value_to_insert);
	}
	idx_t result_length = ListVector::GetListSize(*cum_sum);
	result_data[0].length = result_length;
	result_data[0].offset = result_length;
	ListVector::Append(result, ListVector::GetEntry(*cum_sum), result_length);
	result_data[1].length = result_length;
	result_data[1].offset = result_length;
	ListVector::Append(result, ListVector::GetEntry(*cum_sum), result_length);


}

template <typename NUMERIC_TYPE>
static void ExecuteFlatCumSum(Vector &result, Vector &list_vector, const idx_t count, SelectionVector &sel, idx_t &sel_idx) {
	// Todo: implement Cum Sum for flat vectors
}

template <typename INPUT_TYPE>
static void ExecuteCumSum(Vector &result, Vector &list_vector, const idx_t count) {

	SelectionVector sel;
	idx_t sel_idx = 0;

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		ExecuteConstantCumSum<INPUT_TYPE>(result, list_vector, count, sel, sel_idx);
	} else {
		ExecuteFlatCumSum<INPUT_TYPE>(result, list_vector, count, sel, sel_idx);
	}
	result.Verify(count);
}

static void ListCumSumFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// The CumSum function takes only one argument, the list to on which to apply the cumsum
	D_ASSERT(args.ColumnCount() == 1);
	D_ASSERT(args.data.size() == 1);

	auto count = args.size();

	Vector &list_vector = args.data[0];

	// Todo: Return NULL if the input is NULL ?
	if (list_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		auto &result_validity = FlatVector::Validity(result);
		result_validity.SetInvalid(0);
		return;
	}

	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// todo: I don't know what this does
		if (list_vector.GetVectorType() != VectorType::FLAT_VECTOR &&
		    list_vector.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			list_vector.Flatten(count);
		}
		ListVector::ReferenceEntry(result, list_vector);
		ExecuteCumSum<int>(result, list_vector, count);
		break;
	}
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

ScalarFunctionSet ListCumSumFun::GetFunctions() {

	ScalarFunctionSet set("list_cum_sum");

	// the arguments and return types are actually set in the binder function
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::INTEGER)},
							   LogicalType::LIST(LogicalType::INTEGER), ListCumSumFunction));

	// Todo: add as well for other types

	// Todo: What does this do ? Do I need it ?
	// fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	return set;
}

} // namespace duckdb
