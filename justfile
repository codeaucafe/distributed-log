# Set default recipe to list available commands
default:
    @just --list

# Default generation workflow: format, lint, and generate
gen: format lint generate

# Install buf if not already installed
install-buf:
    @echo "Installing buf..."
    go install github.com/bufbuild/buf/cmd/buf@latest

# Lint protobuf files
lint:
    @echo "Linting protobuf files..."
    buf lint

# Format protobuf files
format:
    @echo "Formatting protobuf files..."
    buf format --write

# Check if files are formatted (CI-friendly)
format-check:
    @echo "Checking protobuf formatting..."
    buf format --diff --exit-code

# Generate Go code from protobuf
generate:
    @echo "Generating Go code from protobuf..."
    buf generate

# Run all checks (lint + format check)
check: lint format-check

# Format and generate (common development workflow)
dev: format generate

# Clean generated files
clean:
    @echo "Cleaning generated protobuf files..."
    find api -name "*.pb.go" -type f -delete
    find api -name "*_grpc.pb.go" -type f -delete
    find api -name "*.connect.go" -type f -delete

# Full workflow: format, lint, generate
all: format lint generate

# Run Go tests with race detection
test:
    @echo "Running Go tests with race detection..."
    go test -race ./...

