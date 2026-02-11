# Safe cleanup script for test-generated artifacts (Windows)
# This script removes temporary files created during test runs

$ErrorActionPreference = "Continue"

function Print-Info($message) {
    Write-Host "ℹ $message" -ForegroundColor Blue
}

function Print-Success($message) {
    Write-Host "✓ $message" -ForegroundColor Green
}

function Print-Warning($message) {
    Write-Host "⚠ $message" -ForegroundColor Yellow
}

function Print-Header($message) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Blue
    Write-Host "  $message" -ForegroundColor Blue
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Blue
    Write-Host ""
}

# Function to safely remove files
function Remove-TestArtifacts {
    param(
        [string[]]$Patterns,
        [string]$Description
    )
    
    $count = 0
    foreach ($pattern in $Patterns) {
        $files = Get-ChildItem -Path . -Filter $pattern -Recurse -File -ErrorAction SilentlyContinue
        foreach ($file in $files) {
            try {
                Remove-Item -Path $file.FullName -Force
                Print-Info "Removed: $($file.FullName)"
                $count++
            }
            catch {
                Print-Warning "Could not remove: $($file.FullName)"
            }
        }
    }
    
    if ($count -gt 0) {
        Print-Success "Removed $count $Description file(s)"
    }
    
    return $count
}

Print-Header "Cleaning Test Artifacts"

$TotalRemoved = 0

# Remove coverage reports
Print-Info "Looking for coverage reports..."
$TotalRemoved += Remove-TestArtifacts -Patterns @(
    "coverage.out",
    "coverage.html",
    "coverage.txt",
    "coverage.xml",
    "coverage-*.out",
    "coverage-*.html"
) -Description "coverage"

# Remove .out files (excluding go.sum which might have .out in path)
Print-Info "Looking for .out files..."
$TotalRemoved += Remove-TestArtifacts -Patterns @("*.out") -Description "test output"

# Remove .tmp files
Print-Info "Looking for .tmp files..."
$TotalRemoved += Remove-TestArtifacts -Patterns @("*.tmp") -Description "temporary"

# Remove test binaries
Print-Info "Looking for test binaries..."
$TotalRemoved += Remove-TestArtifacts -Patterns @("*.test", "*.test.exe") -Description "test binary"

# Remove log files
Print-Info "Looking for test log files..."
$TotalRemoved += Remove-TestArtifacts -Patterns @(
    "test.log",
    "tests.log",
    "*-test.log"
) -Description "test log"

# Remove profile files
Print-Info "Looking for profile files..."
$TotalRemoved += Remove-TestArtifacts -Patterns @(
    "cpu.prof",
    "mem.prof",
    "*.prof"
) -Description "profile"

# Remove benchmark files
Print-Info "Looking for benchmark files..."
$TotalRemoved += Remove-TestArtifacts -Patterns @(
    "bench.out",
    "*.bench"
) -Description "benchmark"

# Summary
Print-Header "Cleanup Summary"

if ($TotalRemoved -gt 0) {
    Print-Success "Cleanup complete! Removed $TotalRemoved file(s)"
}
else {
    Print-Info "No test artifacts found - directory is already clean"
}

Write-Host ""
