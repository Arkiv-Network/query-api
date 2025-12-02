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
)

type IncludeData struct {
	Key                         bool `json:"key"`
	Attributes                  bool `json:"attributes"`
	SyntheticAttributes         bool `json:"syntheticAttributes"`
	Payload                     bool `json:"payload"`
	ContentType                 bool `json:"contentType"`
	Expiration                  bool `json:"expiration"`
	Owner                       bool `json:"owner"`
	CreatedAtBlock              bool `json:"createdAtBlock"`
	LastModifiedAtBlock         bool `json:"lastModifiedAtBlock"`
	TransactionIndexInBlock     bool `json:"transactionIndexInBlock"`
	OperationIndexInTransaction bool `json:"operationIndexInTransaction"`
}

type QueryOptions struct {
	AtBlock        *uint64                   `json:"atBlock"`
	IncludeData    *IncludeData              `json:"includeData"`
	OrderBy        []query.OrderByAnnotation `json:"orderBy"`
	ResultsPerPage uint64                    `json:"resultsPerPage"`
	Cursor         string                    `json:"cursor"`
}

var defaultColumns map[string]string

func init() {
	columns := []string{
		"entity_key",
		"from_block",
	}

	defaultColumns = make(map[string]string, len(columns))
	for _, column := range columns {
		defaultColumns[column] = column
	}
}

func (options *QueryOptions) toInternalQueryOptions() (*internalQueryOptions, error) {
	switch {
	case options == nil:
		return &internalQueryOptions{
			Columns:            defaultColumns,
			IncludeAnnotations: true,
		}, nil
	case options.IncludeData == nil:
		return &internalQueryOptions{
			Columns:            defaultColumns,
			IncludeAnnotations: true,
			OrderBy:            options.OrderBy,
			AtBlock:            options.AtBlock,
			Cursor:             options.Cursor,
		}, nil
	default:
		iq := internalQueryOptions{
			Columns:                     map[string]string{},
			OrderBy:                     options.OrderBy,
			AtBlock:                     options.AtBlock,
			Cursor:                      options.Cursor,
			IncludeAnnotations:          options.IncludeData.Attributes,
			IncludeSyntheticAnnotations: options.IncludeData.SyntheticAttributes,
		}
		if options.IncludeData.Key {
			column := "entity_key"
			iq.Columns[column] = column
		}
		if options.IncludeData.Payload {
			column := "payload"
			iq.Columns[column] = column
		}
		if options.IncludeData.ContentType {
		}
		if options.IncludeData.Expiration {
		}
		if options.IncludeData.Owner {
		}
		if options.IncludeData.CreatedAtBlock {
		}
		if options.IncludeData.LastModifiedAtBlock {
		}
		if options.IncludeData.TransactionIndexInBlock {
		}
		if options.IncludeData.OperationIndexInTransaction {
		}
		return &iq, nil
	}
}

type internalQueryOptions struct {
	AtBlock                     *uint64                   `json:"atBlock"`
	IncludeAnnotations          bool                      `json:"includeAnnotations"`
	IncludeSyntheticAnnotations bool                      `json:"includeSyntheticAnnotations"`
	Columns                     map[string]string         `json:"columns"`
	OrderBy                     []query.OrderByAnnotation `json:"orderBy"`
	Cursor                      string                    `json:"cursor"`
}

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
	op *QueryOptions,
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
	op *QueryOptions,
) (*query.QueryResponse, error) {

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

	options, err := op.toInternalQueryOptions()
	if err != nil {
		return nil, err
	}
	api.log.Info("internal query options", "options", *options)

	latestHead, err := api.store.GetLatestHead(ctx)
	if err != nil {
		return nil, err
	}
	block := latestHead

	queryOptions := query.QueryOptions{
		Log: api.log,

		IncludeAnnotations:          options.IncludeAnnotations,
		IncludeSyntheticAnnotations: options.IncludeSyntheticAnnotations,
		Columns:                     options.Columns,
		OrderBy:                     options.OrderBy,
	}

	if len(options.Cursor) != 0 {
		offset, err := queryOptions.DecodeCursor(options.Cursor)
		if err != nil {
			return nil, err
		}
		block = offset.BlockNumber
		queryOptions.Cursor = offset.ColumnValues
	}

	if options.AtBlock != nil {
		block = *options.AtBlock
	}

	queryOptions.AtBlock = block

	api.log.Info("final query options", "options", queryOptions)

	evaluatedQuery, err := expr.Evaluate(&queryOptions)
	if err != nil {
		return nil, err
	}

	api.log.Info("evaluated query")

	latestCursor := &query.Cursor{}
	response := &query.QueryResponse{
		BlockNumber: block,
		Data:        make([]json.RawMessage, 0),
	}

	err = api.ensureBlockPresent(ctx, block)
	if err != nil {
		return nil, err
	}

	// 256 bytes is for the overhead of the 'envelope' around the entity data
	// and the separator characters in between
	responseSize := 256

	// 512 kb
	maxResponseSize := 512 * 1024 * 1024
	maxResultsPerPage := 0

	if op != nil {
		maxResultsPerPage = int(op.ResultsPerPage)
		api.log.Info("query max results per page", "value", maxResultsPerPage)
	}

	startTime := time.Now()

	defer func() {
		elapsed := time.Since(startTime)
		api.log.Info("query execution time", "elapsed", elapsed)
	}()

	err = api.store.QueryEntitiesInternalIterator(
		ctx,
		evaluatedQuery.Query,
		evaluatedQuery.Args,
		queryOptions,
		func(entity *query.EntityData, cursor *query.Cursor) error {

			ed, err := json.Marshal(entity)
			if err != nil {
				return fmt.Errorf("failed to marshal entity: %w", err)
			}

			// We always add the last obtained result, and set the latest obtained cursor value
			response.Data = append(response.Data, ed)
			latestCursor = cursor

			newLen := responseSize + len(ed) + 1
			if newLen > maxResponseSize {
				return sqlstore.ErrStopIteration
			}

			if maxResultsPerPage > 0 && len(response.Data) >= maxResultsPerPage {
				return sqlstore.ErrStopIteration
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Always include a cursor, the iteration might have terminated because we hit
	// the limit on the query.
	// The client should launch one more request and see whether there are results in it
	cursor, err := queryOptions.EncodeCursor(latestCursor)
	if err != nil {
		return nil, fmt.Errorf("could not encode cursor: %w", err)
	}
	response.Cursor = &cursor

	api.log.Info("query number of results", "value", len(response.Data))
	return response, nil
}
