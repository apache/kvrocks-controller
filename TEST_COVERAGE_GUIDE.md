# Test and Coverage Commands for kvrocks-controller

## Quick Commands

### Option 1: Use the comprehensive test script (Recommended)

**Linux/macOS:**
```bash
bash scripts/run-test-with-coverage.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\run-test-with-coverage.ps1
```

### Option 2: Run individual commands

**Step 1: Run Makefile tests**
```bash
make test
```

**Step 2: Run Go tests with coverage**
```bash
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

**Step 3: View coverage summary**
```bash
go tool cover -func=coverage.out
```

**Step 4: Generate HTML coverage report**
```bash
go tool cover -html=coverage.out -o coverage.html
```

### Option 3: One-liner for quick testing (no setup/teardown)

**With gotestsum (better output):**
```bash
gotestsum --format testname -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./... && go tool cover -func=coverage.out | tail -n 1
```

**Without gotestsum:**
```bash
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./... && go tool cover -func=coverage.out | tail -n 1
```

**Windows PowerShell one-liner:**
```powershell
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...; if ($?) { go tool cover -func=coverage.out | Select-Object -Last 1 }
```

## What Each Flag Does

- `-covermode=atomic`: Ensures thread-safe coverage counting (required with `-race`)
- `-coverprofile=coverage.out`: Outputs coverage data to a file
- `-race`: Enables race detector to find concurrency bugs
- `-p 1`: Runs tests sequentially (one package at a time) to avoid conflicts
- `-v`: Verbose output showing each test as it runs
- `./...`: Runs tests in all packages recursively

## Handling Packages Without Tests

Go automatically skips packages with no test files - you don't need special handling.
If you see `[no test files]`, that's normal and not an error.

## Coverage Output Files

- `coverage.out`: Machine-readable coverage data (used by CI tools)
- `coverage.html`: Human-readable HTML report (open in browser)

## CI Integration

For CI environments, use:
```bash
# GitHub Actions
gotestsum --format github-actions -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...

# GitLab CI
gotestsum --format testname -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...

# Generic CI
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

## Install gotestsum (Optional but Recommended)

```bash
go install gotest.tools/gotestsum@latest
```

Benefits:
- Better formatted output
- Automatic retry of failed tests (if configured)
- Integration with CI platforms
- Saves test output to files

## Makefile Integration

You can add these targets to your Makefile:

```makefile
test-coverage:
	@bash scripts/run-test-with-coverage.sh

test-quick:
	@go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
	@go tool cover -func=coverage.out | tail -n 1
```

Then run:
```bash
make test-coverage  # Full test suite with coverage
make test-quick     # Quick tests without setup/teardown
```
