import subprocess
import json

BINARY_PATH = "/Users/paul/workspace/duckdb/build/release/duckdb"

def run_query(query: str, database: str = None) -> str:
    # if there are " in the query, replace them with \"
    query = query.replace('"', '\\"')

    if database is None:
        database = ':memory:'

    # run the query using duckdb CLI
    result = subprocess.run(
        [BINARY_PATH, database, "-json", "-c", query],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Query failed: {result.stderr}")

    return result.stdout.strip()

def simple_test():

    # Test a simple query
    result = run_query("SELECT 1")
    assert result == '[{"1":1}]'

    # Test a more complex query
    result = run_query("SELECT 1, 2, 3")
    assert result == '[{"1":1,"2":2,"3":3}]'

    # Test a query with a string
    result = run_query("SELECT 'hello' as hello")
    assert result == '[{"hello":"hello"}]'

    # Test a query with a number
    result = run_query("SELECT 42")
    assert result == '[{"42":42}]'


def test_extended_explain(query: str, database: str = None):
    # Test extended explain
    result = run_query(f"PRAGMA disabled_optimizers = 'compressed_materialization';set explain_output = 'all'; EXPLAIN (FORMAT JSON) {query};", database)
    # remove trailing comma from the result
    result = result.rstrip(',\n')
    # surround the result with { and }
    result = f"{{{result}}}"
    print(result)
    json_parsed = json.loads(result)

    # pretty print the JSON
    print(json.dumps(json_parsed, indent=2))


if __name__ == "__main__":
    simple_test()
    # test_extended_explain('SELECT * FROM test as t1 JOIN test as t2 USING (a1)', '/Users/paul/workspace/duckdb/test_my/test.duckdb')
    test_extended_explain('SELECT * FROM test as t1, test as t2 where t1.a1 = t2.a1', '/Users/paul/workspace/duckdb/test_my/test.duckdb')
    print("All tests passed!")