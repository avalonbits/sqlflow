# Contributing

Contributions are welcome. Please read these guidelines before opening a pull request.

## Before you write any code

If you have found a bug, have an improvement suggestion, or want to propose a new feature,
**open an issue first**. Describe the problem or idea clearly so it can be discussed and evaluated.

Pull requests that arrive without a prior accepted issue will be closed without review, no exceptions.
This keeps effort focused and avoids work being done in directions that do not fit the project.

## Opening an issue

- Search existing issues before creating a new one to avoid duplicates.
- For bugs, include the Go version, driver used, and a minimal reproducer.
- For features or improvements, explain the use case and why it belongs in the core library.

## After your issue is accepted

Once an issue is accepted you are welcome to send a pull request. Reference the issue number in the
PR description (e.g. `Closes #42`). Keep the PR focused on the accepted scope — unrelated changes
will be asked to be split out.

## Code style

- Follow standard [Effective Go](https://go.dev/doc/effective_go) conventions.
- Run `goimports` on all modified `.go` files before committing.
- All tests must pass with the race detector enabled: `go test ./... -race`.
- New behaviour must be covered by tests.
