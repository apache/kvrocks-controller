#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Comprehensive test runner with coverage reporting
# This script runs all tests and generates coverage reports

set -e  # Exit on error
set -o pipefail  # Fail if any command in a pipeline fails

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colorized messages
print_info() {
    echo -e "${BLUE}ℹ ${1}${NC}"
}

print_success() {
    echo -e "${GREEN}✓ ${1}${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ ${1}${NC}"
}

print_error() {
    echo -e "${RED}✗ ${1}${NC}"
}

# Print section header
print_section() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  ${1}${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Trap errors and cleanup
cleanup() {
    if [ $? -ne 0 ]; then
        print_error "Tests failed!"
        exit 1
    fi
}
trap cleanup EXIT

# Track overall test status
OVERALL_STATUS=0

# ============================================================================
# STEP 1: Run Makefile tests
# ============================================================================
print_section "Running Makefile Tests (make test)"
print_info "This will run setup, tests, and teardown scripts..."

if make test; then
    print_success "Makefile tests passed!"
else
    print_error "Makefile tests failed!"
    OVERALL_STATUS=1
fi

# ============================================================================
# STEP 2: Run Go tests with coverage (direct command)
# ============================================================================
print_section "Running Go Tests with Coverage"

# Check if gotestsum is available (optional but provides better output)
USE_GOTESTSUM=false
if command -v gotestsum > /dev/null 2>&1; then
    USE_GOTESTSUM=true
    print_info "Using gotestsum for enhanced output"
else
    print_warning "gotestsum not found, using standard go test"
    print_info "Install gotestsum for better output: go install gotest.tools/gotestsum@latest"
fi

# Determine format based on environment
FORMAT="testname"
if [ "$GITHUB_ACTIONS" == "true" ]; then
    FORMAT="github-actions"
    print_info "Running in GitHub Actions mode"
fi

# Run tests with coverage
print_info "Running: go test -covermode=atomic -coverprofile=coverage.out -race -p 1 ./..."
echo ""

if [ "$USE_GOTESTSUM" == "true" ]; then
    # Use gotestsum for better output
    if gotestsum --format "$FORMAT" -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...; then
        print_success "Go tests with coverage passed!"
    else
        print_error "Go tests with coverage failed!"
        OVERALL_STATUS=1
    fi
else
    # Use standard go test with verbose output
    if go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...; then
        print_success "Go tests with coverage passed!"
    else
        print_error "Go tests with coverage failed!"
        OVERALL_STATUS=1
    fi
fi

# ============================================================================
# STEP 3: Generate and display coverage report
# ============================================================================
if [ -f coverage.out ]; then
    print_section "Coverage Report"
    
    print_info "Generating coverage summary..."
    echo ""
    
    # Display coverage by package
    go tool cover -func=coverage.out | tail -n 20
    
    echo ""
    
    # Calculate total coverage
    TOTAL_COVERAGE=$(go tool cover -func=coverage.out | grep total: | awk '{print $3}')
    if [ -n "$TOTAL_COVERAGE" ]; then
        print_success "Total Coverage: $TOTAL_COVERAGE"
    fi
    
    # Optional: Generate HTML coverage report
    print_info "Generating HTML coverage report..."
    if go tool cover -html=coverage.out -o coverage.html; then
        print_success "HTML coverage report generated: coverage.html"
        print_info "Open coverage.html in your browser to view detailed coverage"
    fi
else
    print_warning "No coverage.out file generated"
fi

# ============================================================================
# STEP 4: Final status
# ============================================================================
print_section "Test Summary"

if [ $OVERALL_STATUS -eq 0 ]; then
    print_success "All tests passed successfully! 🎉"
    echo ""
    print_info "Coverage files generated:"
    print_info "  - coverage.out (machine-readable)"
    print_info "  - coverage.html (human-readable)"
    exit 0
else
    print_error "Some tests failed. Please review the output above."
    exit 1
fi
