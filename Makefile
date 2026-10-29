.PHONY: generate web-install web-build build desktop desktop-dev desktop-doctor build-windows build-macos build-linux clean

# 使用项目的 Go 工具链和 Wails 模块版本运行 CLI，防止旧全局 CLI 无法解析新标准库。
WAILS = GOTOOLCHAIN=$$(go env GOVERSION) go run github.com/wailsapp/wails/v2/cmd/wails@$$(go list -m -f '{{.Version}}' github.com/wailsapp/wails/v2)
# 允许桌面构建和开发传入平台参数，例如 -tags webkit2_41。
WAILS_FLAGS ?=

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

# 桌面构建使用当前平台的 Wails 工具链，CLI 的交叉构建目标保持独立。
build: desktop

desktop: generate
	$(WAILS) build $(WAILS_FLAGS)

desktop-dev: generate
	$(WAILS) dev $(WAILS_FLAGS)

desktop-doctor:
	$(WAILS) doctor

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
