#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

template <int64_t MULTIPLIER, class T>
static void FormatBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<T, string_t>(args.data[0], result, args.size(), [&](T bytes) {
		bool is_negative = bytes < 0;
		idx_t unsigned_bytes;
		if (bytes < 0) {
			if (bytes == NumericLimits<T>::Minimum()) {
				unsigned_bytes = idx_t(NumericLimits<T>::Maximum()) + 1;
			} else {
				unsigned_bytes = idx_t(-bytes);
			}
		} else {
			unsigned_bytes = idx_t(bytes);
		}
		return StringVector::AddString(result, (is_negative ? "-" : "") +
		                                           StringUtil::BytesToHumanReadableString(unsigned_bytes, MULTIPLIER));
	});
}

ScalarFunction FormatBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1024, int64_t>);
}

ScalarFunction FormatreadabledecimalsizeFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction<1000,int64_t>);
}

} // namespace duckdb
