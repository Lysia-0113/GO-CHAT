GO ?= go
CONFIG ?= ./config/config.yaml

.PHONY: build run migrate test test-race vet lint tidy fmt

build:
	$(GO) build -o bin/gochat ./cmd/server

run:
	$(GO) run ./cmd/server -config $(CONFIG)

migrate:
	$(GO) run ./cmd/migrate -config $(CONFIG)

test:
	$(GO) test ./... -count=1

test-race:
	$(GO) test ./... -race -count=1

vet:
	$(GO) vet ./...

lint:
	$(GO) vet ./... && $(GO) build ./...

tidy:
	$(GO) mod tidy

fmt:
	gofmt -l -w .
