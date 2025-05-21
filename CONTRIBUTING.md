# Contributing to Stoolap

Thank you for your interest in contributing to Stoolap! This document provides guidelines and information about the contribution process.

## Code of Conduct

By participating in this project, you agree to uphold our Code of Conduct (to be added).

## License Information

Stoolap is licensed under the [Apache License, Version 2.0](LICENSE). By contributing to Stoolap, you agree that your contributions will be licensed under the same license.

### What this means for contributors

1. **Contribution Licensing**: All contributions you submit will be under the Apache License 2.0. If you submit code that is not your original work, please identify its source and confirm it's compatible with the Apache License 2.0.

2. **Contributor License Agreement (CLA)**: We currently do not require a formal CLA, but by submitting a contribution, you are agreeing that your work will be licensed under the project's Apache License 2.0.

3. **Patent Rights**: The Apache License 2.0 includes an express grant of patent rights from contributors to users. Be aware of this when contributing patented intellectual property.

4. **Attribution Requirements**: You must retain all copyright, patent, trademark, and attribution notices from the source form of the work in any derivative works you distribute.

### License Headers

All source files in the project should include a license header. Use the template provided in [HEADER-TEMPLATE.txt](HEADER-TEMPLATE.txt).

For Go files, include the header as a comment at the top of the file:

```go
// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package example
```

When creating new files, please include this header. For existing files, we'll gradually add them during regular maintenance work.

## Development Process

### Setting Up Your Development Environment

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/stoolap.git
   cd stoolap
   ```
3. Add the upstream repository as a remote:
   ```bash
   git remote add upstream https://github.com/stoolap/stoolap.git
   ```
4. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature-or-bugfix-name
   ```

### Testing

Before submitting your changes, please run the tests:

```bash
go test ./...
```

For more comprehensive testing:

```bash
go test -v ./test/...
```

### Submitting Changes

1. Commit your changes using clear commit messages that explain the problem you're solving and your approach
2. Push your branch to your fork
3. Open a pull request against the main repository's `main` branch

### Pull Request Process

1. Update the README.md or documentation with details of your changes, if applicable
2. Ensure all tests pass
3. Your PR will be reviewed by maintainers, who may request changes
4. Once approved, your PR will be merged

## Coding Standards

- Follow Go standard library conventions
- Use standard Go formatting (`go fmt`)
- Run `go vet` to check for common errors
- Keep functions small and focused
- Document public APIs with comments
- Avoid external dependencies

## Additional Resources

- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://golang.org/doc/effective_go.html)
- [Apache License 2.0 Explained](https://tldrlegal.com/license/apache-license-2.0-(apache-2.0))

Thank you for contributing to Stoolap!