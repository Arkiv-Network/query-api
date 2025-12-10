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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := struct {
		dbURL       string
		addr        string
		metricsAddr string
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
			&cli.StringFlag{
				Name:        "metrics-addr",
				Usage:       "Metrics server listen address (empty to disable)",
				EnvVars:     []string{"METRICS_ADDR"},
				Value:       ":9090",
				Destination: &cfg.metricsAddr,
			},
		},
		Action: func(c *cli.Context) error {

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
			defer cancel()

			store, err := sqlstore.NewSQLStore("pgx", cfg.dbURL, log)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %w", err)
			}
			arkivAPI := api.NewArkivAPI(store, log)

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

			// Start metrics server if configured
			var metricsServer *http.Server
			metricsMux := http.NewServeMux()
			metricsMux.Handle("/metrics", promhttp.Handler())
			metricsServer = &http.Server{
				Addr:    cfg.metricsAddr,
				Handler: metricsMux,
			}

			eg, egCtx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				context.AfterFunc(ctx, func() {
					shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer shutdownCtxCancel()

					if metricsServer != nil {
						err := metricsServer.Shutdown(shutdownCtx)
						if err != nil {
							log.Warn("Error shutting down metrics server", "error", err)
						}
						metricsServer.Close()
					}
				})

				log.Info("starting metrics server", "addr", cfg.metricsAddr)
				err := metricsServer.ListenAndServe()
				if err != nil && err != http.ErrServerClosed {
					return fmt.Errorf("metrics server error: %w", err)
				}
				return nil
			})

			eg.Go(func() error {
				context.AfterFunc(egCtx, func() {
					shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer shutdownCtxCancel()

					err := server.Shutdown(shutdownCtx)
					if err != nil {
						log.Warn("Error shutting down server", "error", err)
						server.Close()
					}
				})
				log.Info("starting RPC API server", "addr", cfg.addr)
				err = server.ListenAndServe()
				if err != nil {
					return fmt.Errorf("server error: %w", err)
				}
				return nil
			})

			return eg.Wait()

		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error("Error running API server", "error", err)
		os.Exit(1)
	}
}
