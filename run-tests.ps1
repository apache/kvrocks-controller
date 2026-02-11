# Robust test runner for Windows (Fixed)
$ErrorActionPreference = "Continue"

Write-Host "Running Go tests with coverage..." -ForegroundColor Cyan

# Remove old coverage files
if (Test-Path coverage.out) { Remove-Item coverage.out -Force }

# Run tests without race detector
# We use Start-Process to avoid PowerShell argument parsing quirks with ./...
$process = Start-Process -FilePath "go" -ArgumentList "test", "-v", "-covermode=atomic", "-coverprofile=coverage.out", "./..." -NoNewWindow -PassThru -Wait

if ($process.ExitCode -eq 0) {
    Write-Host ""
    Write-Host "Tests passed!" -ForegroundColor Green
    
    if (Test-Path coverage.out) {
        Write-Host ""
        Write-Host "Coverage Summary:" -ForegroundColor Cyan
        
        go tool cover -func=coverage.out | Select-Object -Last 20
        
        Write-Host ""
        Write-Host "Generating HTML report..." -ForegroundColor Cyan
        go tool cover -html=coverage.out -o coverage.html
        
        Write-Host "Coverage report saved to: coverage.html" -ForegroundColor Green
    }
    
    exit 0
}
else {
    Write-Host ""
    Write-Host "Tests failed!" -ForegroundColor Red
    exit 1
}
