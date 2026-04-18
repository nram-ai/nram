.PHONY: build-server build-ui build dev fresh-start install-ui clean

clean:
	rm -f nram
	go clean ./cmd/server

build-server:
	go build -o ./nram ./cmd/server

ui/node_modules: ui/package-lock.json ui/package.json
	cd ui && npm ci
	@touch ui/node_modules

install-ui: ui/node_modules

build-ui: ui/node_modules
	cd ui && npm run build
	rm -rf internal/ui/dist
	cp -r ui/dist internal/ui/dist

build: build-ui build-server

fresh-start:
	rm -rf nram.db*
	$(MAKE) build

dev:
	cd ui && npm run dev
