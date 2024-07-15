#include <utility>

#pragma once

namespace duckdb {

struct JoinMetrics {
	idx_t n_rows;
	idx_t n_chains;
	vector<vector<int64_t>> ams_sketch;

	JoinMetrics(idx_t n_rows_p, idx_t n_chains_p, vector<vector<int64_t>> ams_sketch_p)
	    : n_rows(n_rows_p), n_chains(n_chains_p), ams_sketch(std::move(ams_sketch_p)) {
	}
};

string ListToJsonString(const vector<vector<int64_t>> &list) {
	string result = "[";
	// no trailing commas
	for (size_t i = 0; i < list.size(); i++) {
		result += "[";
		for (size_t j = 0; j < list[i].size(); j++) {
			result += std::to_string(list[i][j]);
			if (j != list[i].size() - 1) {
				result += ",";
			}
		}
		result += "]";
		if (i != list.size() - 1) {
			result += ",";
		}
	}
	result += "]";
	return result;
}

inline void LogJoinMetrics(JoinMetrics metrics) {
	// check for env var LOG_METRICS_PATH
	const char *log_metrics_path = getenv("LOG_METRICS_DIR");

	// if env var is set, log metrics to file
	if (log_metrics_path) {
		// create to cpp string
		string log_metrics_path_str(log_metrics_path);

		// filename = current timestamp + .log
		uint64_t timestamp =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
		        .count();

		const string log_file_path = log_metrics_path_str + "/" + std::to_string(timestamp) + ".log";
		FILE *log_file = fopen(log_file_path.c_str(), "w");
		if (log_file) {

			const string json_string = "{\"n_rows\": " + std::to_string(metrics.n_rows) +
			                           ", \"n_chains\": " + std::to_string(metrics.n_chains) +
			                           ", \"ams_sketch\": " + ListToJsonString(metrics.ams_sketch) + "}";

			fprintf(log_file, "%s", json_string.c_str());
			fclose(log_file);
		}
	}
}
} // namespace duckdb