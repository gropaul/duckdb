//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_contains_prefilter_function.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

ContainsPrefilterFunctionData::ContainsPrefilterFunctionData(string needle_p, float selectivity_threshold_p,
                                                             idx_t n_vectors_to_check_p)
    : needle(std::move(needle_p)), selectivity_threshold(selectivity_threshold_p),
      n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> ContainsPrefilterFunctionData::Copy() const {
	return make_uniq<ContainsPrefilterFunctionData>(needle, selectivity_threshold, n_vectors_to_check);
}

bool ContainsPrefilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ContainsPrefilterFunctionData>();
	return needle == other.needle;
}

// The prefilter is only useful at the scan level, where ContainsPrefilterExecutor runs the
// k_vert_u32 kernel over the raw vector. Evaluated as a generic expression it would only
// duplicate the exact contains that follows anyway, so it is a no-op.
static idx_t ContainsPrefilterSelect(DataChunk &args, ExpressionState &state, optional_ptr<const SelectionVector> sel,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	return SetAllTrueSelection(args.size(), sel, true_sel, false_sel);
}

ScalarFunction ContainsPrefilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, nullptr, TableFilterFunctions::Bind);
	func.SetSelectCallback(ContainsPrefilterSelect);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(ContainsPrefilterScalarFun::FilterPrune);
	func.SetSerializeCallback(TableFilterFunctionSerialize);
	func.SetDeserializeCallback(TableFilterFunctionDeserialize);
	return func;
}

string ContainsPrefilterScalarFun::ToString(const string &column_name, const string &needle) {
	return "contains_prefilter(" + column_name + ", '" + needle + "')";
}

FilterPropagateResult ContainsPrefilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

unique_ptr<Expression> CreateContainsPrefilterExpression(string needle, const LogicalType &target_type,
                                                         float selectivity_threshold, idx_t n_vectors_to_check) {
	auto function = ContainsPrefilterScalarFun::GetFunction(target_type);
	auto bind_data =
	    make_uniq<ContainsPrefilterFunctionData>(std::move(needle), selectivity_threshold, n_vectors_to_check);
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(target_type, storage_t(0)));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          std::move(bind_data));
}

ScalarFunction TableFilterContainsPrefilterFun::GetFunction() {
	return ContainsPrefilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
