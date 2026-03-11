.PHONY: build-server

build-server:
	@mkdir -p bin
	go build -o bin/nram ./cmd/server
