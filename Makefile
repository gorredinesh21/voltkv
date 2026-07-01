# voltkv Makefile (Linux/macOS/CI). Windows users: use make.ps1 instead.
#
# Static, cgo-free builds so the binary runs in a scratch container.
export CGO_ENABLED=0

BINARY := voltkv
PKG    := ./cmd/server

.PHONY: build test bench run vet clean docker

## build: compile the static server binary
build:
	go build -ldflags "-s -w" -o $(BINARY) $(PKG)

## test: run the full test suite
test:
	go test -timeout 60s ./...

## bench: run the sharding benchmark (proves lock-striping matters)
bench:
	go test -bench BenchmarkSetShards -benchmem ./internal/store

## run: start the server on :6380 with AOF enabled
run:
	go run $(PKG) -addr :6380 -aof voltkv.aof

## vet: static analysis
vet:
	go vet ./...

## clean: remove build + persistence artifacts
clean:
	rm -f $(BINARY) *.aof *.aof.rewrite

## docker: build the container image
docker:
	docker build -t voltkv:latest .
