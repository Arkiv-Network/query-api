package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/Arkiv-Network/query-api/query"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var queryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "arkiv_query_api_query_duration_seconds",
	Help:    "Duration of database queries in seconds",
	Buckets: prometheus.DefBuckets, // Default: .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
})

type arkivAPI struct {
	store *sqlstore.SQLStore
	log   *slog.Logger
}

func NewArkivAPI(store *sqlstore.SQLStore, log *slog.Logger) *arkivAPI {
	return &arkivAPI{
		store: store,
		log:   log,
	}
}

func (api *arkivAPI) Query(
	ctx context.Context,
	req string,
	op *query.Options,
) (*query.QueryResponse, error) {

	// Without this, the RPC handler swallows panics
	// TODO: how does geth handle this??
	defer func() {
		if err := recover(); err != nil {
			api.log.Warn("handler crashed", "error", err)
			// Print a formatted stack trace
			fmt.Fprintf(os.Stderr, "%s\n", debug.Stack())
		}
	}()

	// TODO log query plan
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		queryDuration.Observe(elapsed.Seconds())
		api.log.Info("query execution time", "seconds", elapsed.Seconds(), "query", req)
	}()

	response, err := api.store.QueryEntities(ctx, req, op)

	if err != nil {
		api.log.Warn("error executing query RPC", "error", err)
		return nil, err
	}

	return response, nil
}
