# Contributing to Reminis

Thank you for your interest in contributing to **Reminis**! We welcome contributions of all kinds: bug reports, documentation improvements, architectural feedback, and code contributions.

---

## Code of Conduct

We are committed to providing a friendly, safe, and welcoming environment for all contributors, regardless of experience level. Please be respectful, constructive, and collaborative.

---

## How Can You Contribute?

### 1. Reporting Bugs
If you encounter a bug, unexpected behavior, or an edge case:
1. Check the [existing Issues](https://github.com/SalvucciFacundo/Reminis/issues) to ensure it hasn't already been reported.
2. Open a new issue using a descriptive title.
3. Include reproduction details:
   - Go version (`go version`)
   - Operating system and architecture (`uname -a` or OS version)
   - LLM provider and model used
   - Minimal reproduction steps, command run, or code snippet
   - Relevant error logs (with sensitive keys redacted)

### 2. Suggesting Enhancements
Have an idea for a new tool, adapter, or architecture optimization?
- Open an issue with the tag `enhancement` describing the problem you want to solve, proposed solution, and alternative approaches considered.

### 3. Submitting Pull Requests

#### Development Setup
```bash
# 1. Fork and clone the repository
git clone https://github.com/<your-username>/Reminis.git
cd Reminis

# 2. Create a feature branch
git checkout -b fix/issue-description
# or
git checkout -b feat/new-capability

# 3. Verify tests and race detection pass
go test -count=1 -v -race ./...

# 4. Ensure clean code formatting
go fmt ./...
go vet ./...
```

#### Commit Message Guidelines
We strictly adhere to **Conventional Commits**:
- `fix: resolve race condition in scheduler path locking`
- `feat: add support for custom tool schemas`
- `docs: update MCP server integration instructions`
- `test: add unit test for silence watchdog timeout`
- `refactor: clean up Blackboard pass-by-reference logic`

Do **not** add automated bot trailers or AI co-author signatures (`Co-Authored-By`).

#### Pull Request Checklist
- [ ] Code compiles cleanly with zero warnings (`go vet ./...`).
- [ ] All unit and concurrency race tests pass (`go test -count=1 -race ./...`).
- [ ] New functionality or bug fixes include accompanying test coverage.
- [ ] Documentation (`README.md` or `docs/`) is updated if user-facing behavior changes.

---

## Questions and Discussions

If you have questions about Reminis architecture or want to discuss design decisions, feel free to open a Discussion or Issue on GitHub.
