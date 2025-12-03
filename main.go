package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Arkiv-Network/query-api/api"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	app := &cli.App{
		Name:  "query-api",
		Usage: "Arkiv Query API server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "db-url",
				Usage:    "PostgreSQL connection string",
				EnvVars:  []string{"DB_URL"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "addr",
				Usage:   "HTTP server listen address",
				EnvVars: []string{"ADDR"},
				Value:   ":8087",
			},
		},
		Action: func(c *cli.Context) error {
			dbURL := c.String("db-url")
			addr := c.String("addr")

			store, err := sqlstore.NewSQLStore(dbURL, log)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %w", err)
			}
			arkivAPI := api.NewArkivAPI(store, log)

			log.Info("starting RPC API server", "addr", addr)
			srv := rpc.NewServer()
			srv.RegisterName("arkiv", arkivAPI)
			http.DefaultServeMux.Handle("/api", srv)
			httpsrv := http.Server{Addr: addr, Handler: http.DefaultServeMux}

			if err := httpsrv.ListenAndServe(); err != nil {
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
