package sqlstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Arkiv-Network/query-api/query"
	"github.com/Arkiv-Network/query-api/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStopIteration = errors.New("stop iteration")

type SQLStore struct {
	log *slog.Logger

	pool *pgxpool.Pool
}

func NewSQLStore(dbURL string, log *slog.Logger) (*SQLStore, error) {
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return &SQLStore{
		log:  log,
		pool: pool,
	}, nil
}

func (s *SQLStore) GetLatestHead(ctx context.Context) (uint64, error) {
	row := s.pool.QueryRow(ctx, "SELECT BLOCK FROM last_block LIMIT 1")
	var block uint64
	row.Scan(&block)
	return block, nil
}

func (s *SQLStore) QueryEntitiesInternalIterator(
	ctx context.Context,
	queryStr string,
	args []any,
	options *query.QueryOptions,
	iterator func(*types.EntityData, *types.Cursor) error,
) error {
	s.log.Info("Executing query", "query", queryStr, "args", args)

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		if elapsed.Seconds() > 1 {
			row := s.pool.QueryRow(ctx, fmt.Sprintf("EXPLAIN (FORMAT JSON) %s", queryStr), args...)

			var (
				jsonPlan string
			)

			err := row.Scan(&jsonPlan)
			if err != nil {
				s.log.Error("failed to get query plan", "err", err)
				return
			}

			dst := bytes.Buffer{}
			json.Compact(&dst, []byte(jsonPlan))

			s.log.Info("query plan", "plan", dst.String())
		}
	}()

	queryCtx, queryCtxCancel := context.WithCancel(ctx)
	defer queryCtxCancel()

	rows, err := s.pool.Query(queryCtx, queryStr, args...)
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

		r := types.EntityData{
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
				r.StringAttributes = make([]types.StringAnnotation, 0, len(attrs))
				for k, v := range attrs {
					if options.IncludeData.SyntheticAttributes || !strings.HasPrefix(k, "$") {
						r.StringAttributes = append(r.StringAttributes, types.StringAnnotation{
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
				r.NumericAttributes = make([]types.NumericAnnotation, 0, len(attrs))
				for k, v := range attrs {
					if options.IncludeData.SyntheticAttributes || !strings.HasPrefix(k, "$") {
						r.NumericAttributes = append(r.NumericAttributes, types.NumericAnnotation{
							Key:   k,
							Value: v,
						})
					}
				}
			}
		}

		cursor := types.Cursor{
			BlockNumber:  options.AtBlock,
			ColumnValues: make([]types.CursorValue, 0, len(options.OrderBy)),
		}

		for _, o := range options.OrderBy {
			cursor.ColumnValues = append(cursor.ColumnValues, types.CursorValue{
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
