

namespace duckdb {
class PhysicalFactorizedPreAggreagte : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::FACT_EXPAND;
	};

} // namespace duckdb