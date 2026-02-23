set shell := ["zsh", "-cu"]

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

# Run Go tests without race detection
test:
    @echo "Running Go tests..."
    go test -v ./...

# Run Go tests with race detection
test-race:
    @echo "Running Go tests with race detection..."
    go test -v -race ./...

# TLS certificate paths
TLS_DIR := "tls"
CONFIG_PATH := env_var("HOME") / ".distributed-log"

# Initialize config directory
init:
    mkdir -p {{CONFIG_PATH}}

# Generate TLS certificates
gencert: init
    @echo "Generating CA certificate..."
    cfssl gencert \
        -initca {{TLS_DIR}}/ca-csr.json | cfssljson -bare ca
    @echo "Generating server certificate..."
    cfssl gencert \
        -ca=ca.pem \
        -ca-key=ca-key.pem \
        -config={{TLS_DIR}}/ca-config.json \
        -profile=server \
        {{TLS_DIR}}/server-csr.json | cfssljson -bare server
    cfssl gencert \
        -ca=ca.pem \
        -ca-key=ca-key.pem \
        -config={{TLS_DIR}}/ca-config.json \
        -profile=client \
        {{TLS_DIR}}/client-csr.json | cfssljson -bare root-client
    cfssl gencert \
        -ca=ca.pem \
        -ca-key=ca-key.pem \
        -config={{TLS_DIR}}/ca-config.json \
        -profile=client \
        {{TLS_DIR}}/client-csr.json | cfssljson -bare nobody-client
    mv *.pem *.csr {{CONFIG_PATH}}
    @echo "Certificates generated in {{CONFIG_PATH}}"

# Copy ACL model config to config directory
copy-model: init
    cp internal/auth/model.conf {{CONFIG_PATH}}/model.conf

# Copy ACL policy to config directory
copy-policy: init
    cp internal/auth/policy.csv {{CONFIG_PATH}}/policy.csv

# Copy ACL model and policy to config directory
copy-acl: copy-model copy-policy

