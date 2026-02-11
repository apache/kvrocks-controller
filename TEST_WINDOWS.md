# Quick Test Commands for Windows (Without CGO/Race Detector)

## The Issue
On Windows, the `-race` flag requires CGO to be enabled. If you don't have a C compiler set up,
you can run tests without race detection.

## Quick Commands for Windows

### Option 1: Tests with Coverage (No Race Detection)
```powershell
go test -v -covermode=atomic -coverprofile=coverage.out ./...
```

### Option 2: View Coverage Summary After Tests
```powershell
go test -v -covermode=atomic -coverprofile=coverage.out ./...; if ($?) { go tool cover -func=coverage.out | Select-Object -Last 1 }
```

### Option 3: Generate HTML Coverage Report
```powershell
go test -v -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
Start-Process coverage.html
```

### Option 4: Enable CGO for Race Detection (Requires C Compiler)
```powershell
$env:CGO_ENABLED=1
go test -v -covermode=atomic -coverprofile=coverage.out -race ./...
```

## Recommended: Use WSL or Git Bash for Full Testing

If you need race detection, use WSL (Windows Subsystem for Linux) or Git Bash:

**WSL/Git Bash:**
```bash
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

## All-in-One PowerShell Script

Save this as `test.ps1` and run it:

```powershell
# Remove old coverage files
if (Test-Path coverage.out) { Remove-Item coverage.out }
if (Test-Path coverage.html) { Remove-Item coverage.html }

Write-Host "Running tests with coverage..." -ForegroundColor Blue

# Run tests (without race detector on Windows)
go test -v -covermode=atomic -coverprofile=coverage.out ./...

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✓ Tests passed!" -ForegroundColor Green
    
    Write-Host "`nCoverage Summary:" -ForegroundColor Blue
    go tool cover -func=coverage.out | Select-Object -Last 20
    
    Write-Host "`nGenerating HTML report..." -ForegroundColor Blue
    go tool cover -html=coverage.out -o coverage.html
    
    Write-Host "✓ Coverage report saved to coverage.html" -ForegroundColor Green
    
    # Open in browser
    Start-Process coverage.html
} else {
    Write-Host "`n✗ Tests failed!" -ForegroundColor Red
    exit 1
}
```

Run with:
```powershell
.\test.ps1
```
