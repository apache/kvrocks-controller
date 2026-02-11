# Simple test runner for Windows PowerShell
# This script runs all Go tests with coverage

param(
    [switch]$Quick,
    [switch]$NoRace
)

$ErrorActionPreference = "Continue"

Write-Host "Running Go tests with coverage..." -ForegroundColor Cyan

# Clean up old coverage files
if (Test-Path coverage.out) { Remove-Item coverage.out -Force }
if (Test-Path coverage.html) { Remove-Item coverage.html -Force }

# Build test command
$testArgs = @("-v", "-covermode=atomic", "-coverprofile=coverage.out")

# Add race detector if CGO is available and not disabled
if (-not $NoRace) {
    try {
        $env:CGO_ENABLED = "1"
        $testArgs += "-race"
        Write-Host "Race detector enabled" -ForegroundColor Yellow
    } catch {
        Write-Host "Skipping race detector (CGO not available)" -ForegroundColor Yellow
    }
}

# Add package pattern - use proper escaping for PowerShell
$testArgs += "./..."

Write-Host ""
Write-Host "Command: go test $($testArgs -join ' ')" -ForegroundColor Gray
Write-Host ""

# Run tests
$output = & go test @testArgs 2>&1

# Display output
$output | ForEach-Object { Write-Host $_ }

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✓ All tests passed!" -ForegroundColor Green
    
    # Show coverage summary
    if (Test-Path coverage.out) {
        Write-Host ""
        Write-Host "Coverage Summary:" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
        
        $coverageLines = go tool cover -func=coverage.out
        $coverageLines | Select-Object -Last 20 | ForEach-Object { Write-Host $_ }
        
        # Generate HTML report
        Write-Host ""
        Write-Host "Generating HTML coverage report..." -ForegroundColor Cyan
        go tool cover -html=coverage.out -o coverage.html
        
        if (Test-Path coverage.html) {
            Write-Host "✓ HTML coverage report: coverage.html" -ForegroundColor Green
            Write-Host ""
            
            # Ask to open in browser
            $open = Read-Host "Open coverage.html in browser? (y/n)"
            if ($open -eq 'y' -or $open -eq 'Y') {
                Start-Process coverage.html
            }
        }
    }
    
    exit 0
} else {
    Write-Host ""
    Write-Host "✗ Tests failed!" -ForegroundColor Red
    exit 1
}
