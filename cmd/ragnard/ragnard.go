package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modfin/bellman/services/openai"
	"github.com/modfin/bellman/services/voyageai"
	"github.com/modfin/clix"
	"github.com/modfin/ragnar/internal/ai"
	"github.com/modfin/ragnar/internal/dao"
	"github.com/modfin/ragnar/internal/dao/docket"
	"github.com/modfin/ragnar/internal/storage"
	"github.com/modfin/ragnar/internal/web"
	"github.com/urfave/cli/v3"
)

type Config struct {
	Log        *slog.Logger
	Production bool `cli:"production"`

	DAO     dao.Config
	Storage storage.Config
	Web     web.Config
	Docket  docket.Config
	AI      ai.Config
}

func setLogger(level string, format string) {

	var l = slog.LevelInfo
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "info":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
	if format == "text" {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
	}

	slog.SetDefault(logger)
}

func main() {
	cmd := &cli.Command{
		Name: "ragnar",

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Value:   "info",
				Usage:   "the log level",
				Sources: cli.EnvVars("RAGNAR_LOG_LEVEL"),
			},
			&cli.StringFlag{
				Name:    "log-format",
				Value:   "json",
				Usage:   "the output format [text|json]",
				Sources: cli.EnvVars("RAGNAR_LOG_FORMAT"),
			},

			&cli.StringFlag{
				Name:     "db-uri",
				Required: true,
				Usage:    "the uri used to connect to the database",
				Sources:  cli.EnvVars("RAGNAR_DB_URI"),
			},

			&cli.StringFlag{
				Name:     "s3-endpoint",
				Required: true,
				Usage:    "the s3 endpoint",
				Sources:  cli.EnvVars("RAGNAR_S3_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:     "s3-bucket",
				Required: true,
				Usage:    "the s3 bucket used to store documents",
				Sources:  cli.EnvVars("RAGNAR_S3_BUCKET"),
			},

			&cli.StringFlag{
				Name:     "s3-access-key",
				Required: true,
				Usage:    "the s3 access key",
				Sources:  cli.EnvVars("RAGNAR_S3_ACCESS_KEY"),
			},

			&cli.StringFlag{
				Name:     "s3-secret-key",
				Required: true,
				Usage:    "the s3 secret key",
				Sources:  cli.EnvVars("RAGNAR_S3_SECRET_KEY"),
			},

			&cli.BoolFlag{
				Name:    "production",
				Usage:   "run in production mode",
				Sources: cli.EnvVars("RAGNAR_PRODUCTION"),
			},

			&cli.StringFlag{
				Name:    "http-uri",
				Value:   "http://localhost:7100",
				Usage:   "the base URI for the http server",
				Sources: cli.EnvVars("RAGNAR_HTTP_URI"),
			},
			&cli.IntFlag{
				Name:    "http-port",
				Value:   8080,
				Usage:   "the port for the http server",
				Sources: cli.EnvVars("RAGNAR_HTTP_PORT"),
			},

			&cli.IntFlag{
				Name:    "http-upload-limit",
				Value:   100 << 20, // 100MB
				Usage:   "the maximum size of a post request",
				Sources: cli.EnvVars("RAGNAR_HTTP_POST_LIMIT"),
			},

			&cli.StringFlag{
				Name:    "bellman-uri",
				Usage:   "the base URI for the bellman server",
				Sources: cli.EnvVars("RAGNAR_BELLMAN_URI"),
			},
			&cli.StringFlag{
				Name:    "bellman-name",
				Usage:   "the name of the bellman server",
				Value:   "ragnar",
				Sources: cli.EnvVars("RAGNAR_BELLMAN_NAME"),
			},
			&cli.StringFlag{
				Name:    "bellman-key",
				Usage:   "the api key for the bellman server",
				Sources: cli.EnvVars("RAGNAR_BELLMAN_KEY"),
			},

			&cli.StringFlag{
				Name:    "default-embed-model",
				Usage:   "the default model to use for embedding",
				Value:   voyageai.EmbedModel_voyage_context_3.String(),
				Sources: cli.EnvVars("RAGNAR_DEFAULT_EMBED_MODEL"),
			},
			&cli.StringFlag{
				Name:    "default-gen-model",
				Usage:   "the default model to use for LLM",
				Value:   openai.GenModel_gpt5_mini_latest.String(),
				Sources: cli.EnvVars("RAGNAR_DEFAULT_GEN_MODEL"),
			},
		},

		Commands: []*cli.Command{
			{
				Name: "serve",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					ctx, cancel := context.WithCancel(ctx)
					defer cancel()
					setLogger(cmd.String("log-level"), cmd.String("log-format"))
					l := slog.Default().With("who", "ragnard")

					l.Info("Loading config..")
					cfg := clix.Parse[Config](clix.V3(cmd))
					cfg.Log = slog.Default()

					l.Info("Starting Ragnar service", "prod", cfg.Production)

					l.Info("Creating dao..")
					db, err := dao.New(slog.Default().With("who", "dao"), cfg.DAO, !cfg.Production)
					if err != nil {
						l.Error("failed to create dao", "err", err)
						return err
					}
					ai_ := ai.New(slog.Default().With("who", "ai"), cfg.AI)

					l.Info("Creating storage..")
					stor, err := storage.New(slog.Default().With("who", "storage"), cfg.Storage)
					if err != nil {
						l.Error("failed to create storage", "err", err)
						return err
					}

					l.Info("Creating docket..")
					d, err := docket.New(slog.Default().With("who", "pqdocket"), db, stor, ai_, cfg.Docket)
					if err != nil {
						l.Error("failed to create docket", "err", err)
						return err
					}

					l.Info("Creating web..")
					app := web.New(slog.Default().With("who", "web"), db, stor, d, ai_, cfg.Web)
					if err != nil {
						l.Error("failed to create web", "err", err)
						return err
					}

					signalChannel := make(chan os.Signal, 1)
					signal.Notify(signalChannel, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGINT)

					<-signalChannel
					l.Info("Initiating graceful shutdown..")
					cancel()

					closeCtx, closeCancel := context.WithTimeout(ctx, 10*time.Second)
					defer closeCancel()

					go func() {
						<-signalChannel
						l.Info("Forcing terminate.. os.Exit(1)")
						os.Exit(1)
					}()

					closable := []Closeable{app, stor, d, db, ai_}

					wg := sync.WaitGroup{}

					for _, c := range closable {
						wg.Add(1)
						go func(c Closeable) {
							defer wg.Done()
							l.Info(fmt.Sprintf("Closing %s..", c.Name()))
							err = c.Close(closeCtx)
							if err != nil {
								l.Error(fmt.Sprintf("failed to close %s", c.Name()), "err", err)
							}
							l.Info(fmt.Sprintf("%s is closed", c.Name()))
						}(c)
					}
					wg.Wait()

					return nil
				},
			},
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

type Closeable interface {
	Name() string
	Close(ctx context.Context) error
}
