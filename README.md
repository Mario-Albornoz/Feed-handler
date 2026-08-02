# Feed Handler Aggregator

Real-time market data aggregator with rolling statistics and anomaly detection features.

## Prerequisites

- Go 1.22 or higher
- Kafka broker running (for production use)

## Installation

Install dependencies:

```bash
go mod tidy
```

## Project Structure

```
feed-handler/
├── cmd/
│   └── aggregator/
│       └── main.go              # Application entry point
├── internal/
│   ├── config/
│   │   └── config.go            # Configuration loading and parsing
│   ├── stats/
│   │   ├── rolling.go           # Two-timescale rolling statistics (Welford's algorithm)
│   │   └── rolling_test.go      # Unit tests for rolling stats
│   ├── kafka/                   # Kafka consumer/producer (to be implemented)
│   ├── model/                   # Data models (to be implemented)
│   ├── normalizer/              # Tick normalization logic (to be implemented)
│   └── silence/                 # Silence detection (to be implemented)
├── config/
│   ├── aggregator.yaml          # Main configuration file
│   └── profiles/                # Instrument class profiles
├── .cursor/
│   └── skills/                  # Project-specific Cursor skills
└── go.mod                       # Go module definition
```

## Running Tests

### Run all tests

```bash
go test ./...
```

### Run tests with verbose output

```bash
go test ./... -v
```

### Run tests for a specific package

```bash
# Test rolling statistics
go test ./internal/stats/... -v

# Test config loading
go test ./internal/config/... -v
```

### Run a specific test

```bash
go test ./internal/stats/... -run TestWelfordConvergence -v
```

## Building the Program

Build the aggregator binary:

```bash
go build -o bin/aggregator ./cmd/aggregator
```

## Running the Program

### Run directly with Go

```bash
go run ./cmd/aggregator
```

### Run the compiled binary

```bash
./bin/aggregator
```

The program expects the configuration file at `config/aggregator.yaml`.

## Configuration

Edit `config/aggregator.yaml` to configure:

- Kafka brokers and topics
- Rolling statistics windows (fast/slow)
- CUSUM parameters
- Silence detection settings
- Instrument class profiles

## Development Workflow

### 1. Setting up your development environment

Clone the repository and install dependencies:

```bash
git clone <repository-url>
cd feed-handler
go mod tidy
```

### 2. Run tests before making changes

Always start by ensuring existing tests pass:

```bash
go test ./... -v
```

### 3. Make your changes

Follow the project structure conventions:
- Business logic goes in `internal/`
- Entry points go in `cmd/`
- Configuration files go in `config/`
- Keep packages focused and single-purpose

### 4. Write tests for your changes

Add tests alongside your code (see "Adding Tests" section below).

### 5. Run tests again

```bash
go test ./... -v
```

### 6. Build and verify

```bash
go build -o bin/aggregator ./cmd/aggregator
./bin/aggregator
```

## Adding New Features

### Adding a new internal package

1. Create the package directory under `internal/`:

```bash
mkdir -p internal/yourpackage
```

2. Create your Go file:

```bash
touch internal/yourpackage/yourfile.go
```

3. Define the package:

```go
package yourpackage

// Your code here
```

4. Create corresponding test file:

```bash
touch internal/yourpackage/yourfile_test.go
```

### Adding new configuration options

1. Update the config struct in `internal/config/config.go`:

```go
type AggregatorConfig struct {
    // ... existing fields
    YourNewSection YourNewConfig `yaml:"your_section"`
}

type YourNewConfig struct {
    OptionOne string `yaml:"option_one"`
    OptionTwo int    `yaml:"option_two"`
}
```

2. Update `config/aggregator.yaml`:

```yaml
your_section:
  option_one: "value"
  option_two: 42
```

### Module dependencies

Add new dependencies:

```bash
go get github.com/some/package
```

Update dependencies:

```bash
go get -u ./...
go mod tidy
```

## Adding Tests

### Create a new test file

Test files must end with `_test.go` and be in the same package as the code being tested.

Example structure:

```go
package stats

import "testing"

func TestYourFeature(t *testing.T) {
    // Arrange
    rs := NewRollingStats(60, 14400, 1.0, 0.5)
    
    // Act
    rs.Update(10.0, 0.04, 0.01, 100.0)
    
    // Assert
    if rs.Count != 1 {
        t.Errorf("expected count=1, got %d", rs.Count)
    }
}
```

### Test file naming convention

- Main code: `internal/stats/rolling.go`
- Test code: `internal/stats/rolling_test.go`

### Run your new tests

```bash
go test ./internal/stats/... -v
```

### Table-driven tests

For testing multiple scenarios:

```go
func TestMultipleScenarios(t *testing.T) {
    tests := []struct {
        name     string
        input    float64
        expected float64
    }{
        {"positive value", 10.0, 10.0},
        {"negative value", -5.0, 5.0},
        {"zero value", 0.0, 0.0},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := yourFunction(tt.input)
            if result != tt.expected {
                t.Errorf("got %v, want %v", result, tt.expected)
            }
        })
    }
}
```

## Code Style and Conventions

### Formatting

Format your code before committing:

```bash
go fmt ./...
```

### Linting

Run the linter to catch common issues:

```bash
go vet ./...
```

### Package naming

- Use lowercase, single-word package names
- Package name should match directory name
- Avoid generic names like `util` or `common`

### Error handling

Always handle errors explicitly:

```go
result, err := someOperation()
if err != nil {
    return fmt.Errorf("operation failed: %w", err)
}
```

### Exported vs unexported

- Exported (public): Start with uppercase letter
- Unexported (private): Start with lowercase letter

```go
// Exported - can be used by other packages
type RollingStats struct { ... }

// Unexported - internal to package only
func updateEMA(...) { ... }
```

## Debugging

### Print debugging

```go
import "log"

log.Printf("Debug: value=%v", someValue)
```

### Run with race detector

```bash
go test -race ./...
go run -race ./cmd/aggregator
```

## Common Development Tasks

### Check for compilation errors

```bash
go build ./...
```

### View test coverage

```bash
go test ./... -cover
```

### Generate detailed coverage report

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Clean build cache

```bash
go clean -cache
```
