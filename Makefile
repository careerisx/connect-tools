.PHONY: generate web-install web-build build build-windows build-macos build-linux clean

ifeq ($(OS),Windows_NT)
PROTOC_GEN_ES := web/node_modules/.bin/protoc-gen-es.CMD
else
PROTOC_GEN_ES := web/node_modules/.bin/protoc-gen-es
endif
PROTOC_GEN_ES_STAMP := web/node_modules/.protoc-gen-es.stamp

generate: $(PROTOC_GEN_ES_STAMP)
	mkdir -p gen/go web/src/gen
	protoc --proto_path=proto \
		--plugin=protoc-gen-go="$$(go tool -n protoc-gen-go)" \
		--plugin=protoc-gen-connect-go="$$(go tool -n protoc-gen-connect-go)" \
		--plugin=protoc-gen-es="$(CURDIR)/$(PROTOC_GEN_ES)" \
		--go_out=gen/go --go_opt=paths=source_relative \
		--connect-go_out=gen/go --connect-go_opt=paths=source_relative \
		--es_out=web/src/gen --es_opt=target=ts \
		$(wildcard proto/connecttools/v1/*.proto)

$(PROTOC_GEN_ES_STAMP): web/package.json web/pnpm-lock.yaml
	pnpm --dir web install --frozen-lockfile
	touch $@

web-install:
	pnpm --dir web install --frozen-lockfile

web-build:
	pnpm --dir web build

build-windows: generate web-build
	mkdir -p dist
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/windows-amd64/connect-tools.exe ./cmd/connect-tools

build-macos: generate web-build
	mkdir -p dist
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/darwin-arm64/connect-tools ./cmd/connect-tools

build-linux: generate web-build
	mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/linux-amd64/connect-tools ./cmd/connect-tools

clean:
	go clean

windows-debug: build-windows
	./dist/windows-amd64/connect-tools.exe serve --open
