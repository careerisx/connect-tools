package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"connect-tools/internal/desktop"
)

// version 由桌面构建时的链接参数注入，与 CLI 使用相同的版本约定。
var version = "dev"

// main 在主线程运行 Wails，保留 cmd/connect-tools 下的独立 CLI 入口。
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := desktop.Run(ctx, version); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
