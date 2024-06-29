#pragma once

namespace duckdb {

struct JoinMetrics {
	idx_t n_rows;
	idx_t n_chains;
	double ams_sketch_estimate; // add ams_sketch_estimate field

	JoinMetrics(idx_t n_rows_p, idx_t n_chains_p, double ams_sketch_estimate_p) : n_rows(n_rows_p), n_chains(n_chains_p), ams_sketch_estimate(ams_sketch_estimate_p) {
	}
};

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
			                           	", \"ams_sketch\": " + std::to_string(metrics.ams_sketch_estimate) +"}";

			fprintf(log_file, json_string.c_str(), metrics.n_rows, metrics.n_chains, metrics.ams_sketch_estimate);
			fclose(log_file);
		}
	}
}
} // namespace duckdb