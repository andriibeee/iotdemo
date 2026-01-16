package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/andriibeee/iotdemo/internal/config"
	"github.com/andriibeee/iotdemo/internal/sink"
	"github.com/andriibeee/iotdemo/internal/transport"
	"github.com/andriibeee/iotdemo/pkg/journal"
)

func main() {
	cfgPath := flag.String("config", "", "path to config file")
	flag.Parse()

	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	storage, err := journal.NewFileStorage(cfg.Journal.Dir)
	if err != nil {
		return err
	}

	var journalOpts []journal.Option
	if cfg.Journal.EncryptionKey != "" {
		key, err := base64.StdEncoding.DecodeString(cfg.Journal.EncryptionKey)
		if err != nil {
			return errors.New("invalid encryption key: " + err.Error())
		}
		enc, err := journal.NewAESGCMEncryptor(key)
		if err != nil {
			return errors.New("failed to create encryptor: " + err.Error())
		}
		journalOpts = append(journalOpts, journal.WithEncryptor(enc))
		slog.Info("journal encryption enabled")
	}

	j, err := journal.New(storage, cfg.Journal.MaxSize, journalOpts...)
	if err != nil {
		return err
	}
	defer j.Close()

	var middlewares []sink.Middleware

	if cfg.Dedup.Enabled {
		dedup := sink.NewDeduplicator(cfg.Dedup.CleaningInterval)
		dedup.Start()
		middlewares = append(middlewares, dedup.Middleware())
		slog.Info("dedup enabled", "cleaning_interval", cfg.Dedup.CleaningInterval)
	}

	if cfg.RateLimit.Enabled {
		rl := sink.NewRateLimiter(cfg.RateLimit.BytesPerSec)
		middlewares = append(middlewares, rl.Middleware())
		slog.Info("rate limit enabled", "bytes_per_sec", cfg.RateLimit.BytesPerSec)
	}

	s := sink.New(j,
		sink.WithBufSize(cfg.Sink.BufferSize),
		sink.WithMiddleware(middlewares...),
	)

	go func() {
		if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("sink run error", "error", err)
		}
	}()

	opts := []transport.Option{
		transport.WithAddr(cfg.Server.Addr),
		transport.WithReadTimeout(cfg.Server.ReadTimeout),
		transport.WithWriteTimeout(cfg.Server.WriteTimeout),
	}

	if cfg.Server.TLS.Cert != "" {
		opts = append(opts, transport.WithTLS(cfg.Server.TLS.Cert, cfg.Server.TLS.Key))
	}
	if cfg.Server.TLS.ClientCA != "" {
		opts = append(opts, transport.WithClientCA(cfg.Server.TLS.ClientCA))
	}

	srv := transport.New(s, opts...)

	return srv.Run(ctx)
}
