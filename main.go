package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/Arkiv-Network/query-api/api"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/ethereum/go-ethereum/rpc"
)

type QueryAPI struct {
	log *slog.Logger
}

func (s *QueryAPI) Add(a int, b *int) int {
	if b == nil {
		s.log.Info("Add", "a", a, "b", "nil")
		return a
	}
	s.log.Info("Add", "a", a, "b", *b)
	return a + *b
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// TODO remove password
	store, err := sqlstore.NewSQLStore(
		"host=::1 port=5432 dbname=query user=query password=",
		log,
	)
	if err != nil {
		log.Error("Error running API server", "error", err)
		os.Exit(1)
	}
	api := api.NewArkivAPI(store, log)

	// TODO
	httpAddr := ":8087"

	log.Info("starting RPC API server", "addr", httpAddr)
	srv := rpc.NewServer()
	srv.RegisterName("arkiv", api)
	http.DefaultServeMux.Handle("/api", srv)
	httpsrv := http.Server{Addr: httpAddr, Handler: http.DefaultServeMux}

	err = httpsrv.ListenAndServe()

	if err != nil {
		log.Error("Error running API server", "error", err)
		os.Exit(1)
	}
}
