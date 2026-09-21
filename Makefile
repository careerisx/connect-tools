.PHONY: generate web-install web-build build build-windows build-macos build-linux clean

generate:
	buf generate

web-install:
	pnpm --dir web install --frozen-lockfile

web-build:
	pnpm --dir web build

build: generate web-build
	mkdir -p dist
	go build -trimpath -ldflags "-s -w" -o dist/connect-tools ./cmd/connect-tools

build-windows: generate web-build
	mkdir -p dist
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/connect-tools-windows-amd64.exe ./cmd/connect-tools

build-macos: generate web-build
	mkdir -p dist
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/connect-tools-darwin-arm64 ./cmd/connect-tools
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/connect-tools-darwin-amd64 ./cmd/connect-tools

build-linux: generate web-build
	mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/connect-tools-linux-amd64 ./cmd/connect-tools
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/connect-tools-linux-arm64 ./cmd/connect-tools

clean:
	go clean
