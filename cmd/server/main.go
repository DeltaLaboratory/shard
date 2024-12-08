// cmd/server/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/DeltaLaboratory/shard/internal/server"
	alog "github.com/lesismal/arpc/log"
)

type Config struct {
	NodeID     string
	RaftAddr   string
	ServerAddr string
	DataDir    string
	Bootstrap  bool
	JoinAddr   string
}

// SetLevel(lvl int)
// Debug(format string, v ...interface{})
// Info(format string, v ...interface{})
// Warn(format string, v ...interface{})
// Error(format string, v ...interface{})
type ALogAdapter struct {
	logger zerolog.Logger
}

func (a *ALogAdapter) SetLevel(level int) {
	switch level {
	case alog.LevelDebug:
		a.logger = a.logger.Level(zerolog.DebugLevel)
	case alog.LevelInfo:
		a.logger = a.logger.Level(zerolog.InfoLevel)
	case alog.LevelWarn:
		a.logger = a.logger.Level(zerolog.WarnLevel)
	case alog.LevelError:
		a.logger = a.logger.Level(zerolog.ErrorLevel)
	}
}

func (a *ALogAdapter) Debug(format string, v ...interface{}) {
	a.logger.Debug().Msgf(format, v...)
}

func (a *ALogAdapter) Info(format string, v ...interface{}) {
	a.logger.Info().Msgf(format, v...)
}

func (a *ALogAdapter) Warn(format string, v ...interface{}) {
	a.logger.Warn().Msgf(format, v...)
}

func (a *ALogAdapter) Error(format string, v ...interface{}) {
	a.logger.Error().Msgf(format, v...)
}

func main() {
	logger := zerolog.New(zerolog.NewConsoleWriter()).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	alog.DefaultLogger = &ALogAdapter{logger: logger}

	cfg := parseFlags()

	// Create data directories if they don't exist
	if err := ensureDirectories(cfg.DataDir); err != nil {
		logger.Fatal().Err(err).Msg("Failed to create data directories")
	}

	logger.Info().
		Str("node_id", cfg.NodeID).
		Str("raft_addr", cfg.RaftAddr).
		Str("server_addr", cfg.ServerAddr).
		Str("data_dir", cfg.DataDir).
		Bool("bootstrap", cfg.Bootstrap).
		Str("join_addr", cfg.JoinAddr).
		Msg("Starting server")

	// Create and start server
	srv, err := server.NewServer(
		cfg.NodeID,
		cfg.RaftAddr,
		cfg.ServerAddr,
		cfg.DataDir,
		logger,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create server")
	}

	// Start the server

	go func() {
		if err := srv.Start(); err != nil {
			logger.Fatal().Err(err).Msg("Failed to start server")
		}
	}()

	// Handle bootstrap/join
	if cfg.Bootstrap {
		if err := srv.Bootstrap(); err != nil {
			logger.Fatal().Err(err).Msg("Failed to bootstrap cluster")
		}
		logger.Info().Msg("Successfully bootstrapped cluster")
	} else if cfg.JoinAddr != "" {

		if err := srv.Join(cfg.JoinAddr); err != nil {
			logger.Fatal().Err(err).Msg("Failed to join cluster")
		}
		logger.Info().Msg("Successfully joined cluster")
	}

	// Wait for interrupt signal
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate

	// Graceful shutdown
	logger.Info().Msg("Shutting down server")
	if err := srv.Stop(); err != nil {
		logger.Error().Err(err).Msg("Failed to stop server")
	}
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.NodeID, "id", "", "Node ID (required)")
	flag.StringVar(&cfg.RaftAddr, "raft-addr", "localhost:7000", "Raft TCP address")
	flag.StringVar(&cfg.ServerAddr, "server-addr", "localhost:8000", "Server TCP address")
	flag.StringVar(&cfg.DataDir, "data-dir", "data", "Directory to store data")
	flag.BoolVar(&cfg.Bootstrap, "bootstrap", false, "Bootstrap the cluster")
	flag.StringVar(&cfg.JoinAddr, "join", "", "Join address for existing cluster")

	flag.Parse()

	if cfg.NodeID == "" {
		log.Fatal("Node ID is required")
	}

	if cfg.Bootstrap && cfg.JoinAddr != "" {
		log.Fatal("Cannot both bootstrap and join")
	}

	return cfg
}

func ensureDirectories(dataDir string) error {
	dirs := []string{
		dataDir,
		filepath.Join(dataDir, "raft"),
		filepath.Join(dataDir, "store"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	return nil
}
