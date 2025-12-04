package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/Arkiv-Network/query-api/query"
	"github.com/Arkiv-Network/query-api/sqlstore"
	"github.com/Arkiv-Network/query-api/types"
)

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

func (api *arkivAPI) ensureBlockPresent(ctx context.Context, block uint64) error {
	// In case the query should be run on a block that we don't have yet,
	// we wait for a little bit to see if we receive the block.
	latestHead, err := api.store.GetLatestHead(ctx)
	if err != nil {
		return err
	}

	if block > latestHead {
		// TODO
		//var cadence time.Duration
		//if latestHead <= 1 {
		//	// For block 1, we cannot deduce the cadence, so we just assume 2 seconds
		//	cadence = 2 * time.Second
		//} else {
		//	cadence = time.Duration(
		//		latestsHead.Time-api.eth.blockchain.GetHeaderByHash(latestsHead.ParentHash).Time,
		//	) * time.Second
		//}

		cadence := 2 * time.Second

		time.Sleep(2 * time.Duration(cadence) * time.Second)
		latestHead, err = api.store.GetLatestHead(ctx)
		if err != nil {
			return err
		}
		if block > latestHead {
			return fmt.Errorf("requested block is in the future")
		}
	}
	return nil
}

func (api *arkivAPI) Query(
	ctx context.Context,
	req string,
	op *types.QueryOptions,
) (*types.QueryResponse, error) {

	// Without this, the RPC handler swallows panics
	// TODO: how does geth handle this??
	defer func() {
		if err := recover(); err != nil {
			api.log.Warn("handler crashed", "error", err)
			// Print a formatted stack trace
			fmt.Fprintf(os.Stderr, "%s\n", debug.Stack())
		}
	}()

	response, err := api.doQuery(ctx, req, op)

	if err != nil {
		api.log.Warn("error executing query RPC", "error", err)
		return nil, err
	}

	return response, nil
}

func (api *arkivAPI) doQuery(
	ctx context.Context,
	req string,
	op *types.QueryOptions,
) (*types.QueryResponse, error) {

	if op != nil {
		api.log.Info("query options", "options", *op)
		if op.IncludeData != nil {
			api.log.Info("query options", "includeData", *op.IncludeData)
		}
	}

	expr, err := query.Parse(req, api.log)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	options, err := op.ToInternalQueryOptions()
	if err != nil {
		return nil, err
	}
	api.log.Info("internal query options", "options", *options)

	latestHead, err := api.store.GetLatestHead(ctx)
	if err != nil {
		return nil, err
	}

	queryOptions, err := query.NewQueryOptions(api.log, latestHead, options)
	if err != nil {
		return nil, err
	}

	api.log.Info("final query options", "options", queryOptions)

	evaluatedQuery, err := expr.Evaluate(queryOptions)
	if err != nil {
		return nil, err
	}

	api.log.Info("evaluated query")

	latestCursor := &types.Cursor{}
	response := &types.QueryResponse{
		BlockNumber: queryOptions.AtBlock,
		Data:        make([]json.RawMessage, 0),
	}

	err = api.ensureBlockPresent(ctx, queryOptions.AtBlock)
	if err != nil {
		return nil, err
	}

	// 256 bytes is for the overhead of the 'envelope' around the entity data
	// and the separator characters in between
	responseSize := 256

	// 512 kb
	maxResponseSize := 512 * 1024 * 1024
	maxResultsPerPage := query.QueryResultCountLimit

	if op != nil && op.ResultsPerPage > 0 {
		maxResultsPerPage = op.ResultsPerPage
		api.log.Info("query max results per page", "value", maxResultsPerPage)
	}

	startTime := time.Now()

	defer func() {
		elapsed := time.Since(startTime)
		api.log.Info("query execution time", "elapsed", elapsed)
	}()

	needsCursor := false

	err = api.store.QueryEntitiesInternalIterator(
		ctx,
		evaluatedQuery.Query,
		evaluatedQuery.Args,
		queryOptions,
		func(entity *types.EntityData, cursor *types.Cursor) error {

			ed, err := json.Marshal(entity)
			if err != nil {
				return fmt.Errorf("failed to marshal entity: %w", err)
			}

			// We always add the last obtained result, and set the latest obtained cursor value
			response.Data = append(response.Data, ed)
			latestCursor = cursor

			newLen := responseSize + len(ed) + 1
			if newLen > maxResponseSize || uint64(len(response.Data)) >= maxResultsPerPage {
				needsCursor = true
				return sqlstore.ErrStopIteration
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if needsCursor {
		// Always include a cursor, the iteration might have terminated because we hit
		// the limit on the query.
		// The client should launch one more request and see whether there are more results
		cursor, err := queryOptions.EncodeCursor(latestCursor)
		if err != nil {
			return nil, fmt.Errorf("could not encode cursor: %w", err)
		}
		response.Cursor = &cursor
	}

	api.log.Info("query number of results", "value", len(response.Data))
	return response, nil
}
