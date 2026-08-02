package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/william1nguyen/memkv/internal/app"
)

const defaultConfigPath = "config.yaml"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := run(ctx, os.Args[1:], logger); err != nil {
		logger.Error("application stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, logger *slog.Logger) error {
	flags := flag.NewFlagSet("memkv", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfigPath, "Path to the YAML configuration file")

	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	cfg, err := app.LoadConfig(*configPath)

	if err != nil {
		return err
	}

	application, err := app.New(cfg, logger)

	if err != nil {
		return fmt.Errorf("create application: %w", err)
	}

	logger.Info("starting MemKV", "address", cfg.Server.Address)

	if err := application.Run(ctx); err != nil {
		return fmt.Errorf("run application: %w", err)
	}

	return nil
}
