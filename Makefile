.PHONY: test lint vet cover bench run build fmt

test:
	go test -race ./...

vet:
	go vet ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

bench:
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run

fmt:
	gofmt -w cmd internal pkg

build:
	go build -o bin/agentgate ./cmd/gateway

run:
	go run ./cmd/gateway -c configs/agentgate.example.yaml
