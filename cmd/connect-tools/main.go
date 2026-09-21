package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"connect-tools/internal/cli"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(cli.Execute(ctx, version, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
