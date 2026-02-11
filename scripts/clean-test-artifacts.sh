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
# Safe cleanup script for test-generated artifacts
# This script removes temporary files created during test runs

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}ℹ ${1}${NC}"
}

print_success() {
    echo -e "${GREEN}✓ ${1}${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ ${1}${NC}"
}

echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Cleaning Test Artifacts${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""

# Count files before cleanup
TOTAL_REMOVED=0

# Function to safely remove files matching pattern
safe_remove() {
    local pattern=$1
    local description=$2
    local count=0
    
    # Find and count files
    while IFS= read -r -d '' file; do
        if [ -f "$file" ]; then
            rm -f "$file"
            count=$((count + 1))
            print_info "Removed: $file"
        fi
    done < <(find . -type f -name "$pattern" -print0 2>/dev/null)
    
    if [ $count -gt 0 ]; then
        print_success "Removed $count $description file(s)"
        TOTAL_REMOVED=$((TOTAL_REMOVED + count))
    fi
}

# Remove coverage reports
print_info "Looking for coverage reports..."
safe_remove "coverage.out" "coverage output"
safe_remove "coverage.html" "coverage HTML"
safe_remove "coverage.txt" "coverage text"
safe_remove "coverage.xml" "coverage XML"
safe_remove "coverage-*.out" "coverage profile"
safe_remove "coverage-*.html" "coverage HTML report"

# Remove .out files (test outputs)
print_info "Looking for .out files..."
safe_remove "*.out" "test output"

# Remove .tmp files
print_info "Looking for .tmp files..."
safe_remove "*.tmp" "temporary"

# Remove test binaries
print_info "Looking for test binaries..."
safe_remove "*.test" "test binary"

# Remove log files created during tests
print_info "Looking for log files..."
safe_remove "test.log" "test log"
safe_remove "tests.log" "tests log"
safe_remove "*-test.log" "test log"

# Remove CPU and memory profiles
print_info "Looking for profile files..."
safe_remove "cpu.prof" "CPU profile"
safe_remove "mem.prof" "memory profile"
safe_remove "*.prof" "profile"

# Remove benchmark outputs
print_info "Looking for benchmark files..."
safe_remove "bench.out" "benchmark output"
safe_remove "*.bench" "benchmark"

# Summary
echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
if [ $TOTAL_REMOVED -gt 0 ]; then
    print_success "Cleanup complete! Removed $TOTAL_REMOVED file(s)"
else
    print_info "No test artifacts found - directory is already clean"
fi
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo ""
