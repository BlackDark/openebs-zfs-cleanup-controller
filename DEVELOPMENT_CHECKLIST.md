# Development Checklist

## ⚠️ CRITICAL: Always Complete Before Marking Tasks Done

This checklist MUST be completed before considering any development task finished.

### 🔧 Build Verification
- [ ] `go build ./cmd/controller` - Controller builds successfully
- [ ] `go build ./cmd/cronjob` - Cronjob builds successfully  
- [ ] `go build ./...` - Full project builds successfully
- [ ] No compilation errors or warnings

### 🧪 Test Verification
- [ ] `go test ./...` - All tests pass
- [ ] No test failures or flaky tests
- [ ] New functionality has appropriate test coverage
- [ ] Existing tests still pass after changes

### 📝 Code Quality
- [ ] Code follows Go conventions and best practices
- [ ] All imports are properly organized
- [ ] No unused variables or imports
- [ ] Proper error handling implemented
- [ ] Documentation updated if needed

### 🔍 Functionality Verification
- [ ] New features work as specified in requirements
- [ ] Edge cases are handled appropriately
- [ ] Configuration changes are validated
- [ ] Logging is comprehensive and structured

### 📋 Task Completion
- [ ] All task requirements are met
- [ ] Task status updated to "completed" only after all checks pass
- [ ] Any breaking changes are documented
- [ ] Integration with existing code verified

## 🚨 Never Skip These Steps

**ALWAYS run the full build and test suite before marking any task as complete.**

**If any step fails, fix the issues before proceeding.**

**This checklist helps ensure code quality and prevents regressions.**
