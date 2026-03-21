.PHONY: build-server build-ui build dev install-ui clean

clean:
	rm -f bin/nram
	go clean -cache

build-server:
	@mkdir -p bin
	go build -o bin/nram ./cmd/server

install-ui:
	cd ui && npm ci

build-ui: install-ui
	cd ui && npm run build
	rm -rf internal/ui/dist
	cp -r ui/dist internal/ui/dist

build: build-ui build-server

dev:
	cd ui && npm run dev
