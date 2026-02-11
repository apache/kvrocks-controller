# Comprehensive test runner with coverage reporting for Windows
# This script runs all tests and generates coverage reports

$ErrorActionPreference = "Stop"

# Color functions
function Print-Info($message) {
    Write-Host "ℹ $message" -ForegroundColor Blue
}

function Print-Success($message) {
    Write-Host "✓ $message" -ForegroundColor Green
}

function Print-Warning($message) {
    Write-Host "⚠ $message" -ForegroundColor Yellow
}

function Print-Error($message) {
    Write-Host "✗ $message" -ForegroundColor Red
}

function Print-Section($message) {
    Write-Host ""
    Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Blue
    Write-Host "  $message" -ForegroundColor Blue
    Write-Host "════════════════════════════════════════════════════════════" -ForegroundColor Blue
    Write-Host ""
}

$OverallStatus = 0

# ============================================================================
# STEP 1: Run Makefile tests
# ============================================================================
Print-Section "Running Makefile Tests (make test)"
Print-Info "This will run setup, tests, and teardown scripts..."

try {
    make test
    Print-Success "Makefile tests passed!"
} catch {
    Print-Error "Makefile tests failed!"
    $OverallStatus = 1
}

# ============================================================================
# STEP 2: Run Go tests with coverage
# ============================================================================
Print-Section "Running Go Tests with Coverage"

# Check if gotestsum is available
$UseGotestsum = $false
if (Get-Command gotestsum -ErrorAction SilentlyContinue) {
    $UseGotestsum = $true
    Print-Info "Using gotestsum for enhanced output"
} else {
    Print-Warning "gotestsum not found, using standard go test"
    Print-Info "Install gotestsum for better output: go install gotest.tools/gotestsum@latest"
}

Print-Info "Running: go test -covermode=atomic -coverprofile=coverage.out -race -p 1 ./..."
Write-Host ""

try {
    if ($UseGotestsum) {
        gotestsum --format testname -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
    } else {
        go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
    }
    Print-Success "Go tests with coverage passed!"
} catch {
    Print-Error "Go tests with coverage failed!"
    $OverallStatus = 1
}

# ============================================================================
# STEP 3: Generate and display coverage report
# ============================================================================
if (Test-Path coverage.out) {
    Print-Section "Coverage Report"
    
    Print-Info "Generating coverage summary..."
    Write-Host ""
    
    # Display coverage by package (last 20 lines)
    go tool cover -func=coverage.out | Select-Object -Last 20
    
    Write-Host ""
    
    # Calculate total coverage
    $CoverageOutput = go tool cover -func=coverage.out | Select-String "total:"
    if ($CoverageOutput) {
        $TotalCoverage = ($CoverageOutput -split "\s+")[-1]
        Print-Success "Total Coverage: $TotalCoverage"
    }
    
    # Generate HTML coverage report
    Print-Info "Generating HTML coverage report..."
    go tool cover -html=coverage.out -o coverage.html
    Print-Success "HTML coverage report generated: coverage.html"
    Print-Info "Open coverage.html in your browser to view detailed coverage"
} else {
    Print-Warning "No coverage.out file generated"
}

# ============================================================================
# STEP 4: Final status
# ============================================================================
Print-Section "Test Summary"

if ($OverallStatus -eq 0) {
    Print-Success "All tests passed successfully! 🎉"
    Write-Host ""
    Print-Info "Coverage files generated:"
    Print-Info "  - coverage.out (machine-readable)"
    Print-Info "  - coverage.html (human-readable)"
    exit 0
} else {
    Print-Error "Some tests failed. Please review the output above."
    exit 1
}
