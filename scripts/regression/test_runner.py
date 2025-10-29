import os
import math
import functools
import shutil
from benchmark import BenchmarkRunner, BenchmarkRunnerConfig
from dataclasses import dataclass
from typing import Optional, List, Union
import subprocess

print = functools.partial(print, flush=True)


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Geometric mean of an array of numbers
def geomean(xs):
    if len(xs) == 0:
        return 'EMPTY'
    for entry in xs:
        if not is_number(entry):
            return entry
    return math.exp(math.fsum(math.log(float(x)) for x in xs) / len(xs))


import argparse

# Set up the argument parser
parser = argparse.ArgumentParser(description="Benchmark script with old and new runners.")

# Define the arguments
parser.add_argument("--old", type=str, help="Path to the old runner.", required=True)
parser.add_argument("--new", type=str, help="Path to the new runner.", required=True)
parser.add_argument("--benchmarks", type=str, help="Path to the benchmark file.", required=True)
parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
parser.add_argument("--threads", type=int, help="Number of threads to use.")
parser.add_argument("--memory_limit", type=str, help="Memory limit to use.")
parser.add_argument("--nofail", action="store_true", help="Do not fail on regression.")
parser.add_argument("--disable-timeout", action="store_true", help="Disable timeout.")
parser.add_argument("--max-timeout", type=int, default=3600, help="Set maximum timeout in seconds (default: 3600).")
parser.add_argument("--root-dir", type=str, default="", help="Root directory.")
parser.add_argument("--no-summary", type=str, default=False, help="No summary in the end.")
parser.add_argument("--name", type=str, default="regression", help="Name of the benchmark run.")
parser.add_argument(
    "--gh-summary",
    choices=["disable", "enable", "enable-with-heading"],
    default="disable",
    help="Write results to the GitHub Actions step summary. Choose 'disable', 'enable', or 'enable-with-heading'.",
)
parser.add_argument(
    "--regression-threshold-seconds",
    type=float,
    default=0.05,
    help="REGRESSION_THRESHOLD_SECONDS value for large benchmarks.",
)

# Parse the arguments
args = parser.parse_args()

# Assign parsed arguments to variables
old_runner_path = args.old
new_runner_path = args.new
benchmark_file = args.benchmarks
verbose = args.verbose
threads = args.threads
memory_limit = args.memory_limit
no_regression_fail = args.nofail
disable_timeout = args.disable_timeout
max_timeout = args.max_timeout
root_dir = args.root_dir
no_summary = args.no_summary
regression_threshold_seconds = args.regression_threshold_seconds

# how many times we will run the experiment, to be sure of the regression
NUMBER_REPETITIONS = 5
# the threshold at which we consider something a regression (percentage)
REGRESSION_THRESHOLD_PERCENTAGE = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
REGRESSION_THRESHOLD_SECONDS = regression_threshold_seconds

if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    exit(1)

config_dict = vars(args)
old_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(old_runner_path, benchmark_file, **config_dict))
new_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(new_runner_path, benchmark_file, **config_dict))

benchmark_list = old_runner.benchmark_list

summary = []


@dataclass
class BenchmarkResult:
    benchmark: str
    old_result: Union[float, str]
    new_result: Union[float, str]
    old_failure: Optional[str] = None
    new_failure: Optional[str] = None


multiply_percentage = 1.0 + REGRESSION_THRESHOLD_PERCENTAGE
other_results: List[BenchmarkResult] = []
error_list: List[BenchmarkResult] = []
for i in range(NUMBER_REPETITIONS):
    regression_list: List[BenchmarkResult] = []
    if len(benchmark_list) == 0:
        break
    print(
        f'''====================================================
==============      ITERATION {i}        =============
==============      REMAINING {len(benchmark_list)}        =============
====================================================
'''
    )

    old_results, old_failures = old_runner.run_benchmarks(benchmark_list)
    new_results, new_failures = new_runner.run_benchmarks(benchmark_list)

    for benchmark in benchmark_list:
        old_res = old_results[benchmark]
        new_res = new_results[benchmark]

        old_fail = old_failures[benchmark]
        new_fail = new_failures[benchmark]

        if isinstance(old_res, str) or isinstance(new_res, str):
            # benchmark failed to run - always a regression
            error_list.append(BenchmarkResult(benchmark, old_res, new_res, old_fail, new_fail))
        elif (no_regression_fail == False) and (
            (old_res + REGRESSION_THRESHOLD_SECONDS) * multiply_percentage < new_res
        ):
            regression_list.append(BenchmarkResult(benchmark, old_res, new_res))
        else:
            other_results.append(BenchmarkResult(benchmark, old_res, new_res))
    benchmark_list = [res.benchmark for res in regression_list]

exit_code = 0
regression_list.extend(error_list)
summary = []
if len(regression_list) > 0:
    exit_code = 1
    print(
        '''====================================================
==============  REGRESSIONS DETECTED   =============
====================================================
'''
    )
    for regression in regression_list:
        print(f"{regression.benchmark}")
        print(f"Old timing: {regression.old_result}")
        print(f"New timing: {regression.new_result}")
        if regression.old_failure or regression.new_failure:
            new_data = {
                "benchmark": regression.benchmark,
                "old_failure": regression.old_failure,
                "new_failure": regression.new_failure,
            }
            summary.append(new_data)
        print("")
    print(
        '''====================================================
==============     OTHER TIMINGS       =============
====================================================
'''
    )
else:
    print(
        '''====================================================
============== NO REGRESSIONS DETECTED  =============
====================================================
'''
    )

other_results.sort(key=lambda x: x.benchmark)
for res in other_results:
    print(f"{res.benchmark}")
    print(f"Old timing: {res.old_result}")
    print(f"New timing: {res.new_result}")
    print("")

time_a = geomean(old_runner.complete_timings)
time_b = geomean(new_runner.complete_timings)


import os

print("")  # for spacing in console output

if isinstance(time_a, str) or isinstance(time_b, str):
    old_text = f"Old: {time_a}"
    new_text = f"New: {time_b}"
    delta = 0.0
else:
    delta = time_b - time_a
    percent_change = abs(delta) * 100.0 / max(time_a, time_b)
    if percent_change < 1.0:
        # Less than 1% difference
        old_text = f"Old timing geometric mean: {time_a:.8f} ms"
        new_text = f"New timing geometric mean: {time_b:.8f} ms (±1%)"
    elif time_a > time_b:
        old_text = f"Old timing geometric mean: {time_a:.8f} ms"
        new_text = f"New timing geometric mean: {time_b:.8f} ms (roughly {percent_change:.1f}% faster)"
    else:
        old_text = f"Old timing geometric mean: {time_a:.8f} ms (roughly {percent_change:.1f}% faster)"
        new_text = f"New timing geometric mean: {time_b:.8f} ms"


# Print to console
print(old_text)
print(new_text)

# Write to GitHub Actions summary
if args.gh_summary == "enable" or args.gh_summary == "enable-with-heading":
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            if args.gh_summary == "enable-with-heading":
                # Table header
                f.write("## Benchmark Summary\n\n")
                f.write("| Benchmark | Old (ms) | New (ms) | Δ (%) |\n")
                f.write("|-----------|----------|----------|--------|\n")

            if isinstance(time_a, str) or isinstance(time_b, str):
                # Fallback row for invalid values
                percent_diff = "N/A"
                # we can't have a \n here as github action will add a linebreak between f.write calls
                f.write(f"| `{args.name}` | {time_a} | {time_b} | N/A |")
            else:
                percent_change = delta * 100.0 / max(time_a, time_b)
                f.write(f"| `{args.name}` | {time_a:.8f} | {time_b:.8f} | {percent_change:.2f} |")
    else:
        print("Warning: GITHUB_STEP_SUMMARY not set, skipping summary output.")


# nuke cached benchmark data between runs
if os.path.isdir("duckdb_benchmark_data"):
    shutil.rmtree('duckdb_benchmark_data')

if summary and not no_summary:
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================
'''
    )
    # check the value is "true" otherwise you'll see the prefix in local run outputs
    prefix = "::error::" if ('CI' in os.environ and os.getenv('CI') == 'true') else ""
    for i, failure_message in enumerate(summary, start=1):
        prefix_str = f"{prefix}{i}" if len(prefix) > 0 else f"{i}"
        print(f"{prefix_str}: ", failure_message["benchmark"])
        if failure_message["old_failure"] != failure_message["new_failure"]:
            print("Old:\n", failure_message["old_failure"])
            print("New:\n", failure_message["new_failure"])
        else:
            print(failure_message["old_failure"])
        print("-", 52)

exit(exit_code)
