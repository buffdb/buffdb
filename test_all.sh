#!/bin/bash
# Script to run all tests for BuffDB new features

set -e

echo "=== Running BuffDB Test Suite ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to run test and report result
run_test() {
    local test_name=$1
    local test_command=$2
    
    echo -n "Testing $test_name... "
    if eval "$test_command" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        echo "  Command: $test_command"
        return 1
    fi
}

# Keep track of failures
FAILED_TESTS=0

# Core module tests
echo "1. Core Module Tests"
echo "==================="
run_test "Transaction module" "cargo test --lib transaction:: --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "Index module" "cargo test --lib index:: --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "FTS module" "cargo test --lib fts:: --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "JSON store module" "cargo test --lib json_store:: --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "MVCC module" "cargo test --lib mvcc:: --features vendored-sqlite" || ((FAILED_TESTS++))

echo
echo "2. Integration Tests"
echo "==================="
run_test "Transaction integration" "cargo test --test transaction_test --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "Index integration" "cargo test --test index_test --features vendored-sqlite" || ((FAILED_TESTS++))

echo
echo "3. Backend Tests"
echo "================"
run_test "SQLite transactions" "cargo test sqlite_transaction --features vendored-sqlite" || ((FAILED_TESTS++))

# These might fail due to compilation issues
echo
echo "4. Optional Backend Tests (may fail)"
echo "===================================="
run_test "DuckDB transactions" "cargo test duckdb_transaction --features vendored-duckdb" || echo "  (Expected failure on some systems)"

echo
echo "5. Example Compilation"
echo "====================="
run_test "Advanced features example" "cargo check --example advanced_features --features vendored-sqlite" || ((FAILED_TESTS++))
run_test "Full features demo" "cargo check --example full_features_demo --features vendored-sqlite" || ((FAILED_TESTS++))

echo
echo "6. Documentation Tests"
echo "====================="
run_test "Doc tests" "cargo test --doc --features vendored-sqlite" || ((FAILED_TESTS++))

# Summary
echo
echo "================================="
if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAILED_TESTS tests failed${NC}"
    echo
    echo "To run tests with output:"
    echo "  cargo test --features vendored-sqlite -- --nocapture"
    echo
    echo "To run a specific test:"
    echo "  cargo test test_name --features vendored-sqlite -- --nocapture"
    exit 1
fi