package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Arkiv-Network/query-api/api"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := struct {
		dbURL string
		addr  string
	}{}

	app := &cli.App{
		Name:  "query-api",
		Usage: "Arkiv Query API server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "db-url",
				Usage:       "PostgreSQL connection string",
				EnvVars:     []string{"DB_URL"},
				Required:    true,
				Destination: &cfg.dbURL,
			},
			&cli.StringFlag{
				Name:        "addr",
				Usage:       "HTTP server listen address",
				EnvVars:     []string{"ADDR"},
				Value:       ":8087",
				Destination: &cfg.addr,
			},
		},
		Action: func(c *cli.Context) error {

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
			defer cancel()

			store, err := sqlstore.NewSQLStore(cfg.dbURL, log)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %w", err)
			}
			arkivAPI := api.NewArkivAPI(store, log)

			log.Info("starting RPC API server", "addr", cfg.addr)
			srv := rpc.NewServer()
			err = srv.RegisterName("arkiv", arkivAPI)
			if err != nil {
				return fmt.Errorf("failed to register RPC service: %w", err)
			}

			mux := http.NewServeMux()

			mux.Handle("POST /", srv)
			server := &http.Server{
				Addr:    cfg.addr,
				Handler: mux,
			}

			context.AfterFunc(ctx, func() {
				shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer shutdownCtxCancel()
				err := server.Shutdown(shutdownCtx)
				if err != nil {
					log.Warn("Error shutting down server", "error", err)
					server.Close()
				}
			})

			err = server.ListenAndServe()
			if err != nil {
				return fmt.Errorf("server error: %w", err)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error("Error running API server", "error", err)
		os.Exit(1)
	}
}
