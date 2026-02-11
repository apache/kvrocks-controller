# Complete Test and Coverage Guide

## Summary

I've created comprehensive test and coverage solutions for your kvrocks-controller project. Choose the method that works best for your environment.

---

## ✅ Quick Start (Choose One)

### For Linux/macOS/WSL (Recommended - includes race detection):

```bash
# Option 1: Full test suite with setup/teardown
make test

# Option 2: Comprehensive tests with coverage reporting
make test-coverage

# Option 3: Quick tests without setup/teardown
make test-quick
```

### For Windows PowerShell:

```powershell
# Recommended: Use the PowerShell script
.\run-tests.ps1

# Or run directly (without race detector):
go test -v -covermode=atomic -coverprofile=coverage.out ./...
```

---

## 📋 All Available Methods

### Method 1: Makefile Targets (Linux/macOS/WSL)

#### `make test`
- Runs setup scripts
- Executes all tests
- Runs teardown scripts
- Best for: Integration testing with full environment

#### `make test-coverage` (NEW!)
- Runs setup
- Executes tests with coverage
- Generates HTML report
- Runs teardown
- Best for: Comprehensive testing with coverage analysis

#### `make test-quick` (NEW!)
- Runs tests with coverage
- No setup/teardown
- Shows coverage summary
- Best for: Quick iteration during development

### Method 2: Direct Go Commands

#### Basic test with coverage:
```bash
go test -v -covermode=atomic -coverprofile=coverage.out ./...
```

#### With race detection (Linux/macOS/WSL):
```bash
go test -v -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

#### With gotestsum (better output):
```bash
gotestsum --format testname -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

### Method 3: Custom Scripts

#### Bash (comprehensive):
```bash
bash scripts/run-test-with-coverage.sh
```

#### PowerShell (Windows-friendly):
```powershell
.\run-tests.ps1
```

---

## 🔍 Understanding the Commands

### Flags Explained:

- **`-v`**: Verbose output (shows each test as it runs)
- **`-covermode=atomic`**: Thread-safe coverage counting (required with `-race`)
- **`-coverprofile=coverage.out`**: Save coverage data to file
- **`-race`**: Enable race detector (finds concurrency bugs)
- **`-p 1`**: Run packages sequentially (prevents test conflicts)
- **`./...`**: Test all packages recursively

### Coverage Options:

- **`set`**: Just track if statement was executed (basic)
- **`count`**: Track how many times executed (more detailed)
- **`atomic`**: Thread-safe count (required for `-race`)

---

## 📊 Viewing Coverage Reports

### Quick Summary (terminal):
```bash
go tool cover -func=coverage.out | tail -n 1
```

### Detailed Package Coverage:
```bash
go tool cover -func=coverage.out
```

### HTML Report (visual):
```bash
go tool cover -html=coverage.out -o coverage.html
# Then open coverage.html in your browser
```

---

## 🐛 Common Issues and Solutions

### Issue 1: `-race` requires CGO on Windows

**Error**: `go: -race requires cgo; enable cgo by setting CGO_ENABLED=1`

**Solutions**:
1. Run without `-race` (tests still work, just no race detection):
   ```powershell
   go test -v -covermode=atomic -coverprofile=coverage.out ./...
   ```

2. Enable CGO (requires C compiler like MinGW):
   ```powershell
   $env:CGO_ENABLED=1
   go test -v -covermode=atomic -coverprofile=coverage.out -race ./...
   ```

3. Use WSL or Git Bash for full Linux environment

### Issue 2: `make` command not found on Windows

**Solution**: Use PowerShell scripts or install WSL
```powershell
# Instead of make test, use:
.\run-tests.ps1
```

### Issue 3: "no test files" warnings

**This is normal!** Go automatically skips packages without tests. Not an error.

### Issue 4: Tests hang or timeout

**Solution**: Run packages sequentially with `-p 1`:
```bash
go test -p 1 ./...
```

---

## 🚀 CI/CD Integration

### GitHub Actions:
```yaml
- name: Run tests with coverage
  run: make test-coverage

- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.out
```

### GitLab CI:
```yaml
test:
  script:
    - make test-coverage
  artifacts:
    paths:
      - coverage.out
      - coverage.html
```

### Generic CI:
```bash
# Install gotestsum for better CI output
go install gotest.tools/gotestsum@latest

# Run tests
gotestsum --format github-actions -- -covermode=atomic -coverprofile=coverage.out -race -p 1 ./...
```

---

## 📁 Generated Files

After running tests, you'll have:

- **`coverage.out`**: Machine-readable coverage data
  - Used by CI tools
  - Used by `go tool cover`
  
- **`coverage.html`**: Human-readable HTML report
  - Open in browser
  - Shows line-by-line coverage
  - Color-coded (green = covered, red = not covered)

---

## 🎯 Choosing the Right Command

| Scenario | Command |
|----------|---------|
| Quick local testing | `make test-quick` or `.\run-tests.ps1` |
| Full integration test | `make test` |
| Coverage analysis | `make test-coverage` |
| CI/CD pipeline | `make test` or gotestsum command |
| Development iteration | `go test -v ./package/name` |
| Debugging one test | `go test -v -run TestName ./package` |

---

## 📚 Additional Resources

### Install gotestsum (optional but recommended):
```bash
go install gotest.tools/gotestsum@latest
```

Benefits:
- Better formatted output
- CI integration
- Test result summaries
- Rerun failed tests easily

### Example: Run only failed tests:
```bash
gotestsum --format testname --rerun-fails -- ./...
```

---

## 🎬 Example Workflow

Typical development workflow:

```bash
# 1. Make code changes
vim controller/cluster.go

# 2. Run quick tests
make test-quick

# 3. Check coverage
go tool cover -html=coverage.out -o coverage.html
open coverage.html

# 4. If coverage is low, add tests
vim controller/cluster_test.go

# 5. Run full test suite before commit
make test

# 6. Commit and push
git add .
git commit -m "Add feature X with tests"
git push
```

---

## 📞 Need Help?

- View test output in detail: Add `-v` flag
- Debug specific test: `go test -v -run TestName ./package`
- See all build output: `go test -v -x ./...`
- Profile tests: `go test -cpuprofile=cpu.prof -memprofile=mem.prof ./...`

---

## Files Created

1. **`scripts/run-test-with-coverage.sh`** - Comprehensive bash script
2. **`scripts/run-test-with-coverage.ps1`** - PowerShell version with setup/teardown
3. **`run-tests.ps1`** - Simple PowerShell test runner (no setup/teardown)
4. **`Makefile`** - Updated with `test-coverage` and `test-quick` targets
5. **`TEST_COVERAGE_GUIDE.md`** - Detailed command reference
6. **`TEST_WINDOWS.md`** - Windows-specific instructions
7. **`COMPLETE_GUIDE.md`** (this file) - Everything in one place
