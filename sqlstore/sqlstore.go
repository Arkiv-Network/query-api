package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Arkiv-Network/query-api/query"
	"github.com/ethereum/go-ethereum/common"
)

var ErrStopIteration = errors.New("stop iteration")

type SQLStore struct {
	log *slog.Logger

	db *sql.DB
}

func NewSQLStoreFromDB(db *sql.DB, log *slog.Logger) *SQLStore {
	return &SQLStore{
		log: log,
		db:  db,
	}
}

func NewSQLStore(dbDriver string, dbURL string, log *slog.Logger) (*SQLStore, error) {
	db, err := sql.Open(dbDriver, dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return NewSQLStoreFromDB(db, log), nil
}

func (s *SQLStore) GetLatestHead(ctx context.Context) (uint64, error) {
	row := s.db.QueryRowContext(ctx, "SELECT block FROM last_block LIMIT 1")
	var block uint64
	row.Scan(&block)
	return block, nil
}

func (s *SQLStore) EnsureBlockPresent(ctx context.Context, block uint64) error {
	// In case the query should be run on a block that we don't have yet,
	// we wait for a little bit to see if we receive the block.
	latestHead, err := s.GetLatestHead(ctx)
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
		latestHead, err = s.GetLatestHead(ctx)
		if err != nil {
			return err
		}
		if block > latestHead {
			return fmt.Errorf("requested block is in the future")
		}
	}
	return nil
}

func (s *SQLStore) QueryEntities(
	ctx context.Context,
	req string,
	op *query.Options,
) (*query.QueryResponse, error) {

	if op != nil {
		s.log.Info("query options", "options", *op)
		if op.IncludeData != nil {
			s.log.Info("query options", "includeData", *op.IncludeData)
		}
	}

	expr, err := query.Parse(req, s.log)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	options, err := op.ToInternalQueryOptions()
	if err != nil {
		return nil, err
	}
	s.log.Info("internal query options", "options", *options)

	latestHead, err := s.GetLatestHead(ctx)
	if err != nil {
		return nil, err
	}

	queryOptions, err := query.NewQueryOptions(s.log, latestHead, options)
	if err != nil {
		return nil, err
	}

	s.log.Info("final query options", "options", queryOptions)

	evaluatedQuery, err := expr.Evaluate2(queryOptions)
	if err != nil {
		return nil, err
	}

	s.log.Info("evaluated query")

	latestCursor := &query.Cursor{}
	response := &query.QueryResponse{
		BlockNumber: queryOptions.AtBlock,
		Data:        make([]json.RawMessage, 0),
	}

	err = s.EnsureBlockPresent(ctx, queryOptions.AtBlock)
	if err != nil {
		return nil, err
	}

	maxResultsPerPage := query.QueryResultCountLimit
	if op != nil && op.ResultsPerPage > 0 && op.ResultsPerPage < query.QueryResultCountLimit {
		maxResultsPerPage = op.ResultsPerPage
	}
	s.log.Info("query max results per page", "value", maxResultsPerPage)

	needsCursor := false

	err = s.QueryEntitiesInternalIterator(
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

			newLen := query.ResponseSize + len(ed) + 1
			if newLen > query.MaxResponseSize || uint64(len(response.Data)) >= maxResultsPerPage {
				needsCursor = true
				return ErrStopIteration
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	if needsCursor {
		cursor, err := queryOptions.EncodeCursor(latestCursor)
		if err != nil {
			return nil, fmt.Errorf("could not encode cursor: %w", err)
		}
		response.Cursor = &cursor
	}

	s.log.Info("query number of results", "value", len(response.Data))
	return response, nil
}

func (s *SQLStore) QueryEntitiesInternalIterator(
	ctx context.Context,
	queryStr string,
	args []any,
	options *query.QueryOptions,
	iterator func(*query.EntityData, *query.Cursor) error,
) error {
	s.log.Info("Executing query", "query", queryStr, "args", args)

	queryCtx, queryCtxCancel := context.WithCancel(ctx)
	defer queryCtxCancel()

	rows, err := s.db.QueryContext(queryCtx, queryStr, args...)
	if err != nil {
		return fmt.Errorf("failed to get entities for query: %s: %w", queryStr, err)
	}
	defer rows.Close()

	for rows.Next() {

		err := rows.Err()
		if err != nil {
			return fmt.Errorf("failed to get entities for query: %s: %w", queryStr, err)
		}

		var (
			key            *[]byte
			payload        *[]byte
			fromBlock      *uint64
			owner          *[]byte
			expiresAt      *uint64
			createdAtBlock *uint64
			sequence       *uint64
			numericAttrs   *[]byte
			stringAttrs    *[]byte
		)
		dest := []any{}
		columns := map[string]any{}
		for _, column := range options.Columns {
			switch column.Name {
			case "entity_key":
				dest = append(dest, &key)
				columns[column.Name] = &key
			case "from_block":
				dest = append(dest, &fromBlock)
				columns[column.Name] = &fromBlock
			case "payload":
				dest = append(dest, &payload)
				columns[column.Name] = &payload
			case "owner":
				dest = append(dest, &owner)
				columns[column.Name] = &owner
			case "expires_at":
				dest = append(dest, &expiresAt)
				columns[column.Name] = &expiresAt
			case "created_at_block":
				dest = append(dest, &createdAtBlock)
				columns[column.Name] = &createdAtBlock
			case "sequence":
				dest = append(dest, &sequence)
				columns[column.Name] = &sequence
			case "string_attributes":
				dest = append(dest, &stringAttrs)
				columns[column.Name] = &stringAttrs
			case "numeric_attributes":
				dest = append(dest, &numericAttrs)
				columns[column.Name] = &numericAttrs
			default:
				var value any
				dest = append(dest, &value)
				columns[column.Name] = &value
			}
		}

		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("failed to get entities for query: %s: %w", queryStr, err)
		}

		var keyHash *common.Hash
		if key != nil {
			hash := common.BytesToHash(*key)
			keyHash = &hash
		}
		var value []byte
		if payload != nil {
			value = *payload
		}
		var ownerAddress *common.Address
		if owner != nil {
			addr := common.BytesToAddress(*owner)
			ownerAddress = &addr
		}

		r := query.EntityData{
			Value:     value,
			Owner:     ownerAddress,
			ExpiresAt: expiresAt,
		}

		// We check whether the key was actually requested, since it's always included
		// in the query because of sorting
		if options.IncludeData.Key {
			r.Key = keyHash
		}

		if options.IncludeData.LastModifiedAtBlock && sequence != nil {
			// we need the upper 32 bits
			val := *sequence >> 32
			r.LastModifiedAtBlock = &val
		}
		if options.IncludeData.TransactionIndexInBlock && sequence != nil {
			// we need bits 16 to 32, so we shift right by 16 and then mask with
			// a bit string of 16 ones (subtract one from 2^17)
			val := (*sequence >> 16) & ((1 << 16) - 1)
			r.TransactionIndexInBlock = &val
		}
		if options.IncludeData.OperationIndexInTransaction && sequence != nil {
			// get the lower 16 bits by applying the same bit mask
			val := (*sequence) & ((1 << 16) - 1)
			r.OperationIndexInTransaction = &val
		}

		if options.IncludeData.Attributes {
			if stringAttrs != nil {
				attrs := make(map[string]string)
				err := json.Unmarshal(*stringAttrs, &attrs)
				if err != nil {
					return fmt.Errorf("error unmarshalling string attributes: %w", err)
				}
				r.StringAttributes = make([]query.StringAnnotation, 0, len(attrs))
				for k, v := range attrs {
					if options.IncludeData.SyntheticAttributes || !strings.HasPrefix(k, "$") {
						r.StringAttributes = append(r.StringAttributes, query.StringAnnotation{
							Key:   k,
							Value: v,
						})
					}
				}
			}
			if numericAttrs != nil {
				attrs := make(map[string]uint64)
				err := json.Unmarshal(*numericAttrs, &attrs)
				if err != nil {
					return fmt.Errorf("error unmarshalling string attributes: %w", err)
				}
				r.NumericAttributes = make([]query.NumericAnnotation, 0, len(attrs))
				for k, v := range attrs {
					if options.IncludeData.SyntheticAttributes || !strings.HasPrefix(k, "$") {
						r.NumericAttributes = append(r.NumericAttributes, query.NumericAnnotation{
							Key:   k,
							Value: v,
						})
					}
				}
			}
		}

		cursor := query.Cursor{
			BlockNumber:  options.AtBlock,
			ColumnValues: make([]query.CursorValue, 0, len(options.OrderBy)),
		}

		for _, o := range options.OrderBy {
			cursor.ColumnValues = append(cursor.ColumnValues, query.CursorValue{
				ColumnName: o.Column.Name,
				Value:      columns[o.Column.Name],
				Descending: o.Descending,
			})
		}

		err = iterator(&r, &cursor)
		if errors.Is(err, ErrStopIteration) {
			break
		} else if err != nil {
			return fmt.Errorf("error during query execution: %w", err)
		}
	}

	return nil
}
