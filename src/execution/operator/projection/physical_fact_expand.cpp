#include "duckdb/execution/operator/projection/physical_fact_expand.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

class FactExpandState : public OperatorState {
public:
	explicit FactExpandState(ExecutionContext &context) {
	}

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		// context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

PhysicalFactExpand::PhysicalFactExpand(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::FACT_EXPAND, std::move(types), estimated_cardinality) {
}

OperatorResultType PhysicalFactExpand::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                               GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FactExpandState>();
	// No-op: just reference the input
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFactExpand::ParamsToString() const {
	string extra_info;
	return extra_info;
}

unique_ptr<OperatorState> PhysicalFactExpand::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FactExpandState>(context);
}
} // namespace duckdb