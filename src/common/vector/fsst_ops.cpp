#include "duckdb/common/vector/fsst_ops.hpp"

#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/fsst_vector.hpp"

namespace duckdb {

//! Branchless select loop over the compressed rows: a row matches when it is valid and its compressed
//! bytes compare to the compressed needle according to EQUALITY. NULL rows go to the false side.
template <bool EQUALITY, bool HAS_NULLS, bool HAS_SEL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static idx_t FSSTCompareLoop(const Vector &fsst_vec, const var_binary_t &target, const SelectionVector *sel,
                             const idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	const auto view = FSSTVector::GetDataView(fsst_vec);
	auto &validity = FSSTVector::Validity(fsst_vec);
	idx_t true_count = 0;
	idx_t false_count = 0;

	for (idx_t base_idx = 0; base_idx < count; base_idx++) {
		const bool valid = !HAS_NULLS || validity.RowIsValid(base_idx);
		const var_binary_t row = view.GetVarBinary(base_idx);
		const bool eq = valid && row == target;
		const bool match = eq == EQUALITY && valid;

		const idx_t result_idx = HAS_SEL ? sel->get_index(base_idx) : base_idx;

		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, result_idx);
		}
		true_count += match;
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, result_idx);
		}
		false_count += !match;
	}
	return true_count;
}

template <bool EQUALITY, bool HAS_NULLS, bool HAS_SEL>
static idx_t FSSTCompareSelSwitch(const Vector &fsst_vec, const var_binary_t &target,
                                  optional_ptr<const SelectionVector> sel_opt, idx_t count,
                                  optional_ptr<SelectionVector> true_sel_opt,
                                  optional_ptr<SelectionVector> false_sel_opt) {
	if (true_sel_opt && false_sel_opt) {
		return FSSTCompareLoop<EQUALITY, HAS_NULLS, HAS_SEL, true, true>(fsst_vec, target, sel_opt.get(), count,
		                                                                 true_sel_opt.get(), false_sel_opt.get());
	} else if (true_sel_opt) {
		return FSSTCompareLoop<EQUALITY, HAS_NULLS, HAS_SEL, true, false>(fsst_vec, target, sel_opt.get(), count,
		                                                                  true_sel_opt.get(), false_sel_opt.get());
	} else if (false_sel_opt) {
		return FSSTCompareLoop<EQUALITY, HAS_NULLS, HAS_SEL, false, true>(fsst_vec, target, sel_opt.get(), count,
		                                                                  true_sel_opt.get(), false_sel_opt.get());
	} else {
		return FSSTCompareLoop<EQUALITY, HAS_NULLS, HAS_SEL, false, false>(fsst_vec, target, sel_opt.get(), count,
		                                                                   true_sel_opt.get(), false_sel_opt.get());
	}
}

template <bool EQUALITY, bool HAS_NULLS>
static idx_t FSSTCompareSwitch(const Vector &fsst_vec, const var_binary_t &target,
                               optional_ptr<const SelectionVector> sel_opt, idx_t count,
                               optional_ptr<SelectionVector> true_sel_opt,
                               optional_ptr<SelectionVector> false_sel_opt) {
	if (sel_opt) {
		return FSSTCompareSelSwitch<EQUALITY, HAS_NULLS, true>(fsst_vec, target, sel_opt, count, true_sel_opt,
		                                                       false_sel_opt);
	} else {
		return FSSTCompareSelSwitch<EQUALITY, HAS_NULLS, false>(fsst_vec, target, sel_opt, count, true_sel_opt,
		                                                        false_sel_opt);
	}
}

template <bool EQUALITY>
static bool TryFSSTSelectComparison(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                    idx_t count, optional_ptr<SelectionVector> true_sel,
                                    optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
                                    idx_t &result) {
	const Vector *fsst_side;
	const Vector *const_side;
	if (left.GetVectorType() == VectorType::FSST_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		fsst_side = &left;
		const_side = &right;
	} else if (right.GetVectorType() == VectorType::FSST_VECTOR &&
	           left.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		fsst_side = &right;
		const_side = &left;
	} else {
		return false;
	}
	if (const_side->GetType().InternalType() != PhysicalType::VARCHAR) {
		return false;
	}
	if (ConstantVector::IsNull(*const_side)) {
		// comparison with NULL: leave it to the generic path
		return false;
	}

	// equal strings compress to equal bytes (the encoder is deterministic),
	// so compress the constant once and compare in the compressed domain
	auto needle = *ConstantVector::GetData<string_t>(*const_side);
	auto compressed_needle = FSSTVector::CompressValue(*fsst_side, needle.GetData(), needle.GetSize());
	const var_binary_t target {compressed_needle.data(), compressed_needle.size()};

	auto &validity = FSSTVector::Validity(*fsst_side);
	if (validity.CanHaveNull()) {
		if (null_mask) {
			// the constant is not NULL, so the result is NULL exactly where the input is
			for (idx_t i = 0; i < count; i++) {
				const idx_t row_idx = sel ? sel->get_index(i) : i;
				if (!validity.RowIsValid(row_idx)) {
					null_mask->SetInvalid(row_idx);
				}
			}
		}
		result = FSSTCompareSwitch<EQUALITY, true>(*fsst_side, target, sel, count, true_sel, false_sel);
	} else {
		result = FSSTCompareSwitch<EQUALITY, false>(*fsst_side, target, sel, count, true_sel, false_sel);
	}
	return true;
}

bool FSSTOps::TryEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                        optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                        optional_ptr<ValidityMask> null_mask, idx_t &result) {
	return TryFSSTSelectComparison<true>(left, right, sel, count, true_sel, false_sel, null_mask, result);
}

bool FSSTOps::TryNotEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                           idx_t count, optional_ptr<SelectionVector> true_sel,
                           optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
                           idx_t &result) {
	return TryFSSTSelectComparison<false>(left, right, sel, count, true_sel, false_sel, null_mask, result);
}

} // namespace duckdb
