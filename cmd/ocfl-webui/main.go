package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/srerickson/ocfl-go"
	ocflfs "github.com/srerickson/ocfl-go/fs"
	httpfs "github.com/srerickson/ocfl-go/fs/http"
	"github.com/srerickson/ocfl-go/fs/local"
	ocflS3 "github.com/srerickson/ocfl-go/fs/s3"
	"github.com/srerickson/ocfl-services/access"
	"github.com/srerickson/ocfl-services/access/sqlite"
	"github.com/srerickson/ocfl-services/webui"
)

const envVarRoot = "OCFL_ROOT" // storage root location string

var (
	stopSigs = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
)

func main() {
	err := runServer(os.Args[1:], os.Stderr)
	if err != nil {
		os.Exit(1)
	}
}

// runServer runs the server. it needs to log its own errors
func runServer(args []string, w io.Writer) error {
	ctx, cancel := signal.NotifyContext(context.Background(), stopSigs...)
	defer cancel()
	// Parse command line flags
	flags := struct {
		root  string
		db    string
		addr  string
		debug bool
	}{}
	fs := flag.NewFlagSet("ocfl-server", flag.ContinueOnError)
	fs.SetOutput(w)
	fs.StringVar(&flags.root, "root", "", "OCFL storage root location (file path, s3://bucket/path")
	fs.StringVar(&flags.db, "db", "", "database file path. Defaults to in-memory databases.")
	fs.StringVar(&flags.addr, "addr", ":8283", "server listen port")
	fs.BoolVar(&flags.debug, "debug", false, "more verbose log messages")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var logLevel slog.Level
	if flags.debug {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: logLevel,
	}))
	if flags.root == "" {
		flags.root = os.Getenv(envVarRoot)
	}
	if flags.root == "" {
		err := errors.New("missing required -root flag")
		logger.Error(err.Error())
	}
	if flags.db == "" {
		flags.db = "file::memory:?mode=memory&cache=shared"
	}
	// Parse and initialize OCFL root
	fsys, rootPath, err := parseRootFlag(ctx, flags.root, logger)
	if err != nil {
		err := fmt.Errorf("failed to parse root location %q: %w", flags.root, err)
		logger.Error(err.Error())
		return err
	}
	root, err := ocfl.NewRoot(ctx, fsys, rootPath)
	if err != nil {
		err := fmt.Errorf("failed to initialize OCFL root at %q: %w", flags.root, err)
		logger.Error(err.Error())
		return err
	}
	logger.Info("using OCFL root", "path", flags.root, "ocfl_version", root.Spec())
	// Initialize index database
	db, err := sqlite.NewDB(flags.db)
	if err != nil {
		err := fmt.Errorf("failed to initialize database at %q: %w", flags.db, err)
		logger.Error(err.Error())
		return err
	}
	defer db.Close()
	logger.Info("database initialized", "path", flags.db)
	// Create HTTP server
	service := access.NewService(root, db, flags.root, logger)
	httpServer := &http.Server{
		Addr:    flags.addr,
		Handler: server.New(service),
	}
	// Set up signal handling for graceful shutdown
	serverErrChan := make(chan error, 1)
	go func() {
		logger.Info("starting server", "addr", flags.addr)
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			serverErrChan <- err
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case err := <-serverErrChan:
		logger.Error("server error", "error", err)
		return err
	case <-ctx.Done():
		logger.Info("received shutdown signal")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		// Gracefully shutdown the server
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("server forced to shutdown", "error", err)
			return err
		}
		logger.Info("server gracefully stopped")
		return nil
	}
}

func parseRootFlag(ctx context.Context, loc string, logger *slog.Logger) (ocflfs.FS, string, error) {
	if loc == "" {
		return nil, "", errors.New("location not set")
	}
	locUrl, err := url.Parse(loc)
	if err != nil {
		return nil, "", err
	}
	switch locUrl.Scheme {
	case "s3":
		bucket := locUrl.Host
		prefix := strings.TrimPrefix(locUrl.Path, "/")
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, "", err
		}
		s3Client := s3.NewFromConfig(cfg)
		fsys := &ocflS3.BucketFS{S3: s3Client, Bucket: bucket, Logger: logger}
		return fsys, prefix, nil
	case "http", "https":
		fsys := httpfs.New(loc)
		return fsys, ".", nil
	default:
		absPath, err := filepath.Abs(loc)
		if err != nil {
			return nil, "", err
		}
		fsys, err := local.NewFS(absPath)
		if err != nil {
			return nil, "", err
		}
		return fsys, ".", nil
	}
}
