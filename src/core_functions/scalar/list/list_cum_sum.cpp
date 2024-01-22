
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/core_functions/scalar/list_functions.hpp"

namespace duckdb {

template <typename NUMERIC_TYPE>
static void ExecuteCumSum(Vector &result, Vector &list_vector, const idx_t count) {

	auto list_count = ListVector::GetListSize(list_vector);
	auto &list_child = ListVector::GetEntry(list_vector);

	D_ASSERT(list_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(list_child).CheckAllValid(list_count)) {
		throw InvalidInputException("list_cum_sum: argument can not contain NULL values");
	}

	auto list_data = FlatVector::GetData<NUMERIC_TYPE>(list_child);
	idx_t current_offset = list_count;

	UnaryExecutor::Execute<list_entry_t, list_entry_t>(list_vector, result, count, [&](list_entry_t list) {
		auto dimensions = list.length;

		NUMERIC_TYPE sum = 0;
		vector<NUMERIC_TYPE> cum_sum_vector(dimensions);

		NUMERIC_TYPE *list_ptr = list_data + list.offset;

		for (idx_t i = 0; i < dimensions; i++) {
			NUMERIC_TYPE list_element = list_ptr[i];

			sum += list_element;
			cum_sum_vector[i] = sum;
		}

		list_entry_t result_entry {};
		auto cum_sum = make_uniq<Vector>(LogicalType::LIST(list_child.GetType()));
		for (auto val : cum_sum_vector) {
			Value value_to_insert = Value::Numeric(list_child.GetType(), val);
			ListVector::PushBack(*cum_sum, value_to_insert);
		}

		idx_t result_length = ListVector::GetListSize(*cum_sum);
		result_entry.length = result_length;
		result_entry.offset = current_offset;

		current_offset += result_length;
		ListVector::Append(result, ListVector::GetEntry(*cum_sum), result_length);

		return result_entry;
	});

	result.Verify(count);
}
template <typename NUMERIC_TYPE>
static void ListCumSumFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// The CumSum function takes only one argument, the list to on which to apply the cumsum
	D_ASSERT(args.ColumnCount() == 1);

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
		ExecuteCumSum<NUMERIC_TYPE>(result, list_vector, count);
		break;
	}
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

ScalarFunctionSet ListCumSumFun::GetFunctions() {

	ScalarFunctionSet set("list_cum_sum");

	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::INTEGER)}, LogicalType::LIST(LogicalType::INTEGER),
	                               ListCumSumFunction<uint32_t>));

	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::BIGINT)}, LogicalType::LIST(LogicalType::BIGINT),
	                               ListCumSumFunction<int64_t>));

	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT)}, LogicalType::LIST(LogicalType::FLOAT),
	                               ListCumSumFunction<float>));

	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE)}, LogicalType::LIST(LogicalType::DOUBLE),
	                               ListCumSumFunction<double>));


	// Todo: What does this do ? Do I need it ?
	// fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	return set;
}

} // namespace duckdb
