#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/simd_utils.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/storage/compression/fsst/fsst_mandatory_chain.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

static unique_ptr<ExpressionFilterExecutor> TryCreateFastExecutor(ClientContext &context, const Expression &expression,
                                                                  bool inside_selectivity_optional);

static void InitializeExecutor(ClientContext &context, const Expression &expression, ExpressionFilterState &state) {
	state.executor = make_uniq<ExpressionExecutor>(context);
	state.executor->AddExpression(expression);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	fast_executor = TryCreateFastExecutor(context, expression, false);
	InitializeExecutor(context, expression, *this);
}

ExpressionFilterState::~ExpressionFilterState() {
}

// Dictionaries larger than this are not worth pre-evaluating per entry (mirrors the threshold of
// the executor's dictionary expression optimization).
static constexpr idx_t MAX_FILTER_DICTIONARY_SIZE = 20000;

idx_t ExpressionFilterExecutor::FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
                                                idx_t &approved_tuple_count) {
	if (approved_tuple_count == 0) {
		return 0;
	}
	if (vector.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return FilterSelectionInternal(sel, vector, scan_count, approved_tuple_count);
	}
	const auto dictionary_size = DictionaryVector::DictionarySize(vector);
	auto &dictionary_id = DictionaryVector::DictionaryId(vector);
	if (!dictionary_size.IsValid() || dictionary_id.empty() ||
	    dictionary_size.GetIndex() >= MAX_FILTER_DICTIONARY_SIZE ||
	    DictionaryVector::Child(vector).GetVectorType() != VectorType::FLAT_VECTOR) {
		// not a storage dictionary we can pre-evaluate per entry
		return FilterSelectionInternal(sel, vector, scan_count, approved_tuple_count);
	}
	if (dictionary_id != cached_dictionary_id) {
		ComputeDictionaryVerdicts(vector, dictionary_size.GetIndex());
		cached_dictionary_id = dictionary_id;
	}
	// keep the rows whose dictionary entry passed the filter
	auto &dictionary_sel = DictionaryVector::SelVector(vector);
	if (dictionary_result_capacity < approved_tuple_count) {
		dictionary_result_sel.Initialize(approved_tuple_count);
		dictionary_result_capacity = approved_tuple_count;
	}
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		const auto row_idx = sel.get_index(i);
		dictionary_result_sel.set_index(result_count, row_idx);
		result_count += dictionary_verdicts[dictionary_sel.get_index(row_idx)];
	}
	sel.Initialize(dictionary_result_sel);
	approved_tuple_count = result_count;
	return result_count;
}

void ExpressionFilterExecutor::ComputeDictionaryVerdicts(Vector &dictionary_vector, idx_t dictionary_size) {
	auto &child = DictionaryVector::Child(dictionary_vector);
	dictionary_verdicts.assign(dictionary_size, 0);
	// evaluate the filter over the unique entries, one STANDARD_VECTOR_SIZE block at a time
	for (idx_t offset = 0; offset < dictionary_size; offset += STANDARD_VECTOR_SIZE) {
		const auto block_count = MinValue<idx_t>(dictionary_size - offset, STANDARD_VECTOR_SIZE);
		Vector block(child, offset, offset + block_count);
		SelectionVector block_sel;
		idx_t block_approved = block_count;
		FilterSelectionInternal(block_sel, block, block_count, block_approved);
		for (idx_t i = 0; i < block_approved; i++) {
			dictionary_verdicts[offset + block_sel.get_index(i)] = 1;
		}
	}
}

// Fallback child of a mixed conjunction: evaluates one conjunct through the regular
// ExpressionExecutor, so that sibling conjuncts with kernel executors keep the fast path.
class GenericFilterExecutor final : public ExpressionFilterExecutor {
public:
	GenericFilterExecutor(ClientContext &context, const Expression &expression) {
		executor = make_uniq<ExpressionExecutor>(context);
		executor->AddExpression(expression);
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		PrepareCapacity(approved_tuple_count);
		DataChunk chunk;
		chunk.data.emplace_back(Vector::Ref(vector));
		chunk.SetChildCardinality(scan_count);
		SelectionVector identity_sel;
		optional_ptr<SelectionVector> current_sel = &sel;
		if (!sel.IsSet()) {
			identity_sel = SelectionVector::Incremental(approved_tuple_count);
			current_sel = &identity_sel;
		}
		approved_tuple_count = executor->SelectExpression(chunk, result_sel, current_sel, approved_tuple_count);
		sel.Initialize(result_sel);
		return approved_tuple_count;
	}

private:
	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		result_sel.Initialize(count);
		current_capacity = count;
	}

	unique_ptr<ExpressionExecutor> executor;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
};

class ConjunctionAndFilterExecutor final : public ExpressionFilterExecutor {
public:
	explicit ConjunctionAndFilterExecutor(vector<unique_ptr<ExpressionFilterExecutor>> children_p)
	    : children(std::move(children_p)) {
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		for (auto &child : children) {
			child->FilterSelection(sel, vector, scan_count, approved_tuple_count);
			if (approved_tuple_count == 0) {
				break;
			}
		}
		return approved_tuple_count;
	}

private:
	vector<unique_ptr<ExpressionFilterExecutor>> children;
};

class OptionalFilterExecutor final : public ExpressionFilterExecutor {
public:
	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		return approved_tuple_count;
	}
};

class ComparisonFilterExecutor final : public ExpressionFilterExecutor {
public:
	ComparisonFilterExecutor(ExpressionType comparison_type_p, Value constant_p)
	    : comparison_type(comparison_type_p), constant(std::move(constant_p)) {
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		(void)scan_count;
		if (approved_tuple_count == 0) {
			return 0;
		}
		PrepareCapacity(approved_tuple_count);
		const auto before_count = approved_tuple_count;
		if (constant.IsNull()) {
			approved_tuple_count = 0;
		} else {
			UnifiedVectorFormat vdata;
			vector.ToUnifiedFormat(vdata);
			auto &current_sel = sel.IsSet() ? sel : *FlatVector::IncrementalSelectionVector();
			approved_tuple_count = Select(vdata, current_sel, approved_tuple_count);
		}
		if (approved_tuple_count != before_count) {
			sel.Initialize(result_sel);
		}
		return approved_tuple_count;
	}

private:
	template <class T, class OP, bool CAN_HAVE_NULL>
	idx_t TemplatedSelect(const UnifiedVectorFormat &vdata, T predicate, const SelectionVector &sel,
	                      idx_t approved_tuple_count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		auto &mask = vdata.validity;
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			auto vector_idx = vdata.sel->get_index(idx);
			bool comparison_result =
			    (!CAN_HAVE_NULL || mask.RowIsValid(vector_idx)) && OP::Operation(data[vector_idx], predicate);
			result_sel.set_index(result_count, idx);
			result_count += comparison_result;
		}
		return result_count;
	}

	template <class T, class OP>
	idx_t SelectOperation(const UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t approved_tuple_count) {
		auto predicate = constant.GetValueUnsafe<T>();
		if (vdata.validity.CannotHaveNull()) {
			return TemplatedSelect<T, OP, false>(vdata, predicate, sel, approved_tuple_count);
		}
		return TemplatedSelect<T, OP, true>(vdata, predicate, sel, approved_tuple_count);
	}

	template <class T>
	idx_t SelectTyped(const UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t approved_tuple_count) {
		switch (comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return SelectOperation<T, Equals>(vdata, sel, approved_tuple_count);
		case ExpressionType::COMPARE_NOTEQUAL:
			return SelectOperation<T, NotEquals>(vdata, sel, approved_tuple_count);
		case ExpressionType::COMPARE_GREATERTHAN:
			return SelectOperation<T, GreaterThan>(vdata, sel, approved_tuple_count);
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return SelectOperation<T, GreaterThanEquals>(vdata, sel, approved_tuple_count);
		case ExpressionType::COMPARE_LESSTHAN:
			return SelectOperation<T, LessThan>(vdata, sel, approved_tuple_count);
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return SelectOperation<T, LessThanEquals>(vdata, sel, approved_tuple_count);
		default:
			throw InternalException("Unsupported comparison type for comparison filter executor");
		}
	}

	idx_t Select(const UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t approved_tuple_count) {
		switch (constant.type().InternalType()) {
		case PhysicalType::BOOL:
			return SelectTyped<bool>(vdata, sel, approved_tuple_count);
		case PhysicalType::INT8:
			return SelectTyped<int8_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::INT16:
			return SelectTyped<int16_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::INT32:
			return SelectTyped<int32_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::INT64:
			return SelectTyped<int64_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::INT128:
			return SelectTyped<hugeint_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::UINT8:
			return SelectTyped<uint8_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::UINT16:
			return SelectTyped<uint16_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::UINT32:
			return SelectTyped<uint32_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::UINT64:
			return SelectTyped<uint64_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::UINT128:
			return SelectTyped<uhugeint_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::FLOAT:
			return SelectTyped<float>(vdata, sel, approved_tuple_count);
		case PhysicalType::DOUBLE:
			return SelectTyped<double>(vdata, sel, approved_tuple_count);
		case PhysicalType::INTERVAL:
			return SelectTyped<interval_t>(vdata, sel, approved_tuple_count);
		case PhysicalType::VARCHAR:
			return SelectTyped<string_t>(vdata, sel, approved_tuple_count);
		default:
			throw InternalException("Unsupported physical type for comparison filter executor");
		}
	}

	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		result_sel.Initialize(count);
		current_capacity = count;
	}

	ExpressionType comparison_type;
	Value constant;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
};

class SelectivityOptionalFilterExecutor final : public ExpressionFilterExecutor {
public:
	SelectivityOptionalFilterExecutor(unique_ptr<ExpressionFilterExecutor> child_p, idx_t n_vectors_to_check,
	                                  float selectivity_threshold)
	    : child(std::move(child_p)), stats(n_vectors_to_check, selectivity_threshold) {
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!child) {
			return approved_tuple_count;
		}
		if (!stats.IsActive()) {
			stats.Update(0, 0);
			return approved_tuple_count;
		}
		const auto before_count = approved_tuple_count;
		child->FilterSelection(sel, vector, scan_count, approved_tuple_count);
		stats.Update(approved_tuple_count, before_count);
		return approved_tuple_count;
	}

private:
	unique_ptr<ExpressionFilterExecutor> child;
	SelectivityOptionalFilterState::SelectivityStats stats;
};

class BloomFilterExecutor final : public ExpressionFilterExecutor {
public:
	BloomFilterExecutor(const BloomFilterFunctionData &data, bool inside_selectivity_optional)
	    : filter(data.filter), filters_null_values(data.filters_null_values), hashes(LogicalType::HASH) {
		if (!inside_selectivity_optional && data.n_vectors_to_check != 0) {
			stats = make_uniq<SelectivityOptionalFilterState::SelectivityStats>(data.n_vectors_to_check,
			                                                                    data.selectivity_threshold);
		}
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!filter || !filter->IsInitialized()) {
			return approved_tuple_count;
		}
		if (stats && !stats->IsActive()) {
			stats->Update(0, 0);
			return approved_tuple_count;
		}

		const auto hash_capacity = sel.IsSet() ? scan_count : approved_tuple_count;
		PrepareCapacity(hash_capacity);
		if (sel.IsSet()) {
			VectorOperations::Hash(vector, hashes, sel, approved_tuple_count);
			if (hashes.GetVectorType() == VectorType::FLAT_VECTOR) {
				FlatVector::SetSize(hashes, count_t(hash_capacity));
			}
		} else {
			VectorOperations::Hash(vector, hashes, approved_tuple_count);
		}

		optional_ptr<const SelectionVector> active_sel = sel.IsSet() ? &sel : nullptr;
		const auto bloom_count = ProbeBloomFilter(active_sel, approved_tuple_count);
		if (filters_null_values || bloom_count == approved_tuple_count) {
			if (stats) {
				stats->Update(bloom_count, approved_tuple_count);
			}
			TranslateSelection(sel, bloom_count, approved_tuple_count);
			approved_tuple_count = bloom_count;
			return approved_tuple_count;
		}

		auto result_count = AddNullsToBloomSelection(sel, vector, bloom_count, approved_tuple_count);
		if (stats) {
			stats->Update(result_count, approved_tuple_count);
		}
		approved_tuple_count = result_count;
		return approved_tuple_count;
	}

private:
	idx_t ProbeBloomFilter(optional_ptr<const SelectionVector> sel, idx_t approved_tuple_count) {
		if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			const auto hash = *ConstantVector::GetData<hash_t>(hashes);
			return filter->LookupOne(hash) ? approved_tuple_count : 0;
		}
		if (hashes.GetVectorType() != VectorType::FLAT_VECTOR) {
			hashes.Flatten();
		}
		D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
		if (sel) {
			return filter->LookupHashes(hashes, *sel, bloom_sel, approved_tuple_count);
		}
		return filter->LookupHashes(hashes, bloom_sel, approved_tuple_count);
	}

	idx_t AddNullsToBloomSelection(SelectionVector &sel, Vector &vector, idx_t bloom_count,
	                               idx_t approved_tuple_count) {
		UnifiedVectorFormat input_data;
		vector.ToUnifiedFormat(input_data);
		if (input_data.validity.CannotHaveNull()) {
			TranslateSelection(sel, bloom_count, approved_tuple_count);
			return bloom_count;
		}

		PrepareResultCapacity(approved_tuple_count);
		idx_t result_count = 0;
		idx_t bloom_idx = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			const auto matched = bloom_idx < bloom_count && bloom_sel.get_index_unsafe(bloom_idx) == i;
			if (matched) {
				bloom_idx++;
			}
			const auto idx = sel.IsSet() ? sel.get_index(i) : i;
			const auto input_idx = input_data.sel->get_index(idx);
			bool passed;
			if (!input_data.validity.RowIsValid(input_idx)) {
				passed = !filters_null_values;
			} else {
				passed = matched;
			}
			if (passed) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		return result_count;
	}

	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		hashes.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		bloom_sel.Initialize(count);
		current_capacity = count;
	}

	void PrepareResultCapacity(idx_t count) {
		if (result_capacity >= count) {
			return;
		}
		result_sel.Initialize(count);
		result_capacity = count;
	}

	void TranslateSelection(SelectionVector &sel, idx_t result_count, idx_t approved_tuple_count) {
		if (result_count == approved_tuple_count) {
			return;
		}
		if (sel.IsSet()) {
			for (idx_t i = 0; i < result_count; i++) {
				const auto flat_sel_idx = bloom_sel.get_index(i);
				const auto original_sel_idx = sel.get_index(flat_sel_idx);
				sel.set_index(i, original_sel_idx);
			}
		} else {
			sel.Initialize(bloom_sel);
		}
	}

	optional_ptr<BloomFilter> filter;
	bool filters_null_values;
	Vector hashes;
	SelectionVector bloom_sel;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
	idx_t result_capacity = 0;
	unique_ptr<SelectivityOptionalFilterState::SelectivityStats> stats;
};

class PrefixRangeFilterExecutor final : public ExpressionFilterExecutor {
public:
	PrefixRangeFilterExecutor(const PrefixRangeFunctionData &data, bool inside_selectivity_optional)
	    : filter(data.filter), filters_null_values(data.filters_null_values) {
		if (!inside_selectivity_optional && data.n_vectors_to_check != 0) {
			stats = make_uniq<SelectivityOptionalFilterState::SelectivityStats>(data.n_vectors_to_check,
			                                                                    data.selectivity_threshold);
		}
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!filter || !filter->IsInitialized()) {
			return approved_tuple_count;
		}
		if (stats && !stats->IsActive()) {
			stats->Update(0, 0);
			return approved_tuple_count;
		}

		PrepareCapacity(approved_tuple_count);
		const auto has_selection = sel.IsSet();
		const auto result_count = has_selection ? filter->LookupKeys(vector, sel, local_sel, approved_tuple_count)
		                                        : filter->LookupKeys(vector, local_sel, approved_tuple_count);
		idx_t final_count;
		if (!filters_null_values && result_count != approved_tuple_count) {
			final_count = AddNullsToSelection(sel, vector, result_count, approved_tuple_count);
		} else {
			final_count = result_count;
			if (result_count != approved_tuple_count && has_selection) {
				TranslateSelection(sel, result_count);
			} else if (result_count != approved_tuple_count) {
				sel.Initialize(local_sel);
			}
		}
		if (stats) {
			stats->Update(final_count, approved_tuple_count);
		}
		approved_tuple_count = final_count;
		return approved_tuple_count;
	}

private:
	idx_t AddNullsToSelection(SelectionVector &sel, Vector &vector, idx_t filter_count, idx_t approved_tuple_count) {
		UnifiedVectorFormat input_data;
		vector.ToUnifiedFormat(input_data);
		if (input_data.validity.CannotHaveNull()) {
			if (sel.IsSet()) {
				TranslateSelection(sel, filter_count);
			} else if (filter_count != approved_tuple_count) {
				sel.Initialize(local_sel);
			}
			return filter_count;
		}

		idx_t result_count = 0;
		idx_t filter_idx = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			const auto matched = filter_idx < filter_count && local_sel.get_index_unsafe(filter_idx) == i;
			filter_idx += matched;
			const auto idx = sel.IsSet() ? sel.get_index(i) : i;
			const auto input_idx = input_data.sel->get_index(idx);
			if (matched || !input_data.validity.RowIsValid(input_idx)) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		return result_count;
	}

	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		local_sel.Initialize(count);
		result_sel.Initialize(count);
		current_capacity = count;
	}

	void TranslateSelection(SelectionVector &sel, idx_t result_count) {
		for (idx_t i = 0; i < result_count; i++) {
			result_sel.set_index(i, sel.get_index(local_sel.get_index(i)));
		}
		sel.Initialize(result_sel);
	}

	optional_ptr<PrefixRangeFilter> filter;
	bool filters_null_values;
	SelectionVector local_sel;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
	unique_ptr<SelectivityOptionalFilterState::SelectivityStats> stats;
};

// Contains prefilter: one k_vert_u32 scan per vector, dropping rows that definitely do not
// contain the needle. On FSST vectors it scans the compressed codes for the first CODE_LEN codes
// of the needle's mandatory chain; on flat vectors it scans for the needle's CODE_LEN-byte prefix.
// Survivors have false positives and anything the kernel cannot handle passes through untouched -
// the exact contains downstream is the correctness authority.
class ContainsPrefilterExecutor final : public ExpressionFilterExecutor {
public:
	ContainsPrefilterExecutor(const ContainsPrefilterFunctionData &data, bool inside_selectivity_optional)
	    : needle(data.needle) {
		if (!inside_selectivity_optional && data.n_vectors_to_check != 0) {
			stats = make_uniq<SelectivityOptionalFilterState::SelectivityStats>(data.n_vectors_to_check,
			                                                                    data.selectivity_threshold);
		}
	}

	idx_t FilterSelectionInternal(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                              idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (needle.size() < CODE_LEN) {
			return approved_tuple_count;
		}
		if (stats && !stats->IsActive()) {
			stats->Update(0, 0);
			return approved_tuple_count;
		}
		const auto before_count = approved_tuple_count;
		idx_t result_count;
		const bool fsst_executable = FilterFSST(sel, vector, before_count, result_count);

		if (!fsst_executable) {
			vector.Flatten();
			result_count = FilterFlat(sel, vector, before_count);
		}
		if (stats) {
			stats->Update(result_count, before_count);
		}
		approved_tuple_count = result_count;
		return approved_tuple_count;
	}

private:
	bool FilterFSST(SelectionVector &sel, Vector &vector, const idx_t count, idx_t &result_count) {
		if (vector.GetVectorType() != VectorType::FSST_VECTOR) {
			return false;
		}
		const auto decoder = FSSTVector::GetDecoder(vector);
		if (!decoder) {
			return false;
		}
		if (decoder != cached_decoder) {
			cached_chain = FSSTMandatoryChain(decoder, needle.data(), needle.size());
			cached_decoder = decoder;
		}
		if (cached_chain.size() < CODE_LEN) {
			return false;
		}
		char pattern[CODE_LEN];
		memcpy(pattern, cached_chain.data(), CODE_LEN);
		PrepareCapacity(count);
		const auto base = FSSTVector::GetBasePointer(vector);
		const auto offsets = FSSTVector::GetOffsets(vector);
		auto &validity = FSSTVector::Validity(vector);
		result_count = k_vert_u32(base, offsets, validity, sel, result_sel, count, pattern);
		sel.Initialize(result_sel);
		return true;
	}

	idx_t FilterFlat(SelectionVector &sel, Vector &vector, idx_t count) {
		char pattern[CODE_LEN];
		memcpy(pattern, needle.data(), CODE_LEN);
		PrepareCapacity(count);
		const auto strings = FlatVector::GetData<string_t>(vector);
		auto &validity = FlatVector::Validity(vector);
		const auto result_count = k_vert_u32(strings, validity, sel, result_sel, count, pattern);
		sel.Initialize(result_sel);
		return result_count;
	}

	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		result_sel.Initialize(count);
		current_capacity = count;
	}

	string needle;
	//! Mandatory chain cache, keyed on the segment's decoder
	void *cached_decoder = nullptr;
	vector<uint8_t> cached_chain;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
	unique_ptr<SelectivityOptionalFilterState::SelectivityStats> stats;
};

static bool IsColumnReferenceFunction(const BoundFunctionExpression &func) {
	auto &children = func.GetChildren();
	if (children.size() != 1 || children[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	auto &ref = children[0]->Cast<BoundReferenceExpression>();
	return ref.Index() == 0;
}

static bool IsColumnReferenceExpression(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	auto &ref = expr.Cast<BoundReferenceExpression>();
	return ref.Index() == 0;
}

static bool IsSupportedComparisonType(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

static unique_ptr<ExpressionFilterExecutor> TryCreateComparisonExecutor(const BoundFunctionExpression &func) {
	auto comparison_type = func.GetExpressionType();
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		break;
	default:
		return nullptr;
	}

	auto &left = BoundComparisonExpression::Left(func);
	auto &right = BoundComparisonExpression::Right(func);
	optional_ptr<const BoundConstantExpression> constant;
	optional_ptr<const Expression> column;
	if (IsColumnReferenceExpression(left) && right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column = &left;
		constant = &right.Cast<BoundConstantExpression>();
	} else if (IsColumnReferenceExpression(right) && left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		column = &right;
		comparison_type = FlipComparisonExpression(comparison_type);
		constant = &left.Cast<BoundConstantExpression>();
	}
	if (!constant) {
		return nullptr;
	}
	const auto column_type = column->GetReturnType().InternalType();
	const auto constant_type = constant->GetValue().type().InternalType();
	if (column_type != constant_type || !IsSupportedComparisonType(column_type)) {
		return nullptr;
	}
	return make_uniq<ComparisonFilterExecutor>(comparison_type, constant->GetValue());
}

static unique_ptr<ExpressionFilterExecutor> TryCreateFunctionExecutor(ClientContext &context,
                                                                      const BoundFunctionExpression &func,
                                                                      bool inside_selectivity_optional) {
	if (!inside_selectivity_optional && func.GetChildren().size() == 2 &&
	    BoundComparisonExpression::IsComparison(func.GetExpressionType())) {
		return TryCreateComparisonExecutor(func);
	}
	if (!IsColumnReferenceFunction(func)) {
		return nullptr;
	}

	const auto &func_name = func.Function().GetName();
	if (func_name == OptionalFilterScalarFun::NAME) {
		return make_uniq<OptionalFilterExecutor>();
	}
	if (func_name == SelectivityOptionalFilterScalarFun::NAME) {
		if (!func.BindInfo()) {
			return make_uniq<OptionalFilterExecutor>();
		}
		auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
		if (!data.child_filter_expr) {
			return make_uniq<OptionalFilterExecutor>();
		}
		auto child = TryCreateFastExecutor(context, *data.child_filter_expr, true);
		if (!child) {
			return nullptr;
		}
		return make_uniq<SelectivityOptionalFilterExecutor>(std::move(child), data.n_vectors_to_check,
		                                                    data.selectivity_threshold);
	}
	if (func_name == BloomFilterScalarFun::NAME) {
		if (!func.BindInfo()) {
			return nullptr;
		}
		return make_uniq<BloomFilterExecutor>(func.BindInfo()->Cast<BloomFilterFunctionData>(),
		                                      inside_selectivity_optional);
	}
	if (func_name == PrefixRangeScalarFun::NAME) {
		if (!func.BindInfo()) {
			return nullptr;
		}
		return make_uniq<PrefixRangeFilterExecutor>(func.BindInfo()->Cast<PrefixRangeFunctionData>(),
		                                            inside_selectivity_optional);
	}
	if (func_name == ContainsPrefilterScalarFun::NAME) {
		if (!func.BindInfo()) {
			return nullptr;
		}
		return make_uniq<ContainsPrefilterExecutor>(func.BindInfo()->Cast<ContainsPrefilterFunctionData>(),
		                                            inside_selectivity_optional);
	}
	return nullptr;
}

static unique_ptr<ExpressionFilterExecutor> TryCreateFastExecutor(ClientContext &context, const Expression &expression,
                                                                  bool inside_selectivity_optional) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return TryCreateFunctionExecutor(context, expression.Cast<BoundFunctionExpression>(),
		                                 inside_selectivity_optional);
	case ExpressionClass::BOUND_CONJUNCTION: {
		if (expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			return nullptr;
		}
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		auto &child_exprs = conjunction.GetChildren();
		vector<unique_ptr<ExpressionFilterExecutor>> children(child_exprs.size());
		idx_t fast_count = 0;
		for (idx_t child_idx = 0; child_idx < child_exprs.size(); child_idx++) {
			children[child_idx] = TryCreateFastExecutor(context, *child_exprs[child_idx], inside_selectivity_optional);
			fast_count += children[child_idx] != nullptr;
		}
		if (fast_count == 0) {
			// no conjunct has a kernel executor: leave the whole expression to the regular executor
			return nullptr;
		}
		// mixed conjunction: conjuncts without a kernel executor run through the regular executor
		for (idx_t child_idx = 0; child_idx < child_exprs.size(); child_idx++) {
			if (!children[child_idx]) {
				children[child_idx] = make_uniq<GenericFilterExecutor>(context, *child_exprs[child_idx]);
			}
		}
		return make_uniq<ConjunctionAndFilterExecutor>(std::move(children));
	}
	default:
		return nullptr;
	}
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
