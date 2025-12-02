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
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStopIteration = errors.New("stop iteration")

type SQLStore struct {
	log *slog.Logger

	pool *pgxpool.Pool
}

func NewSQLStore(dbURL string, log *slog.Logger) (*SQLStore, error) {

	log.Info("Connecting to database", "url", dbURL)
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
	options query.QueryOptions,
	iterator func(*query.EntityData, *query.Cursor) error,
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
			key       *[]byte
			payload   *[]byte
			fromBlock *uint64
		)
		dest := []any{}
		columns := map[string]any{}
		for _, column := range options.AllColumns() {
			switch column {
			case "entity_key":
				dest = append(dest, &key)
				columns[column] = &key
			case "from_block":
				dest = append(dest, &fromBlock)
				columns[column] = &fromBlock
			case "payload":
				dest = append(dest, &payload)
				columns[column] = &payload
			default:
				var value any
				dest = append(dest, &value)
				columns[column] = &value
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

		r := query.EntityData{
			Value:             value,
			StringAttributes:  []query.StringAnnotation{},
			NumericAttributes: []query.NumericAnnotation{},
		}

		// We check whether the key was actually requested, since it's always included
		// in the query because of sorting
		_, wantsKey := options.Columns["entity_key"]
		if wantsKey {
			r.Key = keyHash
		}

		cursor := query.Cursor{
			BlockNumber:  options.AtBlock,
			ColumnValues: make([]query.CursorValue, 0, len(options.OrderByColumns())),
		}

		for _, column := range options.OrderByColumns() {
			cursor.ColumnValues = append(cursor.ColumnValues, query.CursorValue{
				ColumnName: column.Name,
				Value:      columns[column.Name],
				Descending: column.Descending,
			})
		}

		if options.IncludeAnnotations {
			s.log.Info("fetching string attributes")
			strAttrRows, err := s.pool.Query(
				queryCtx,
				"SELECT key, value FROM string_attributes WHERE entity_key = $1 AND from_block = $2",
				keyHash.Bytes(),
				fromBlock,
			)
			if err != nil {
				return fmt.Errorf("error fetching string attributes: %w", err)
			}
			strAttrs, err := pgx.CollectRows(strAttrRows, pgx.RowToStructByName[query.StringAnnotation])
			if err != nil {
				return fmt.Errorf("error fetching string attributes: %w", err)
			}

			// Convert string annotations
			for _, attr := range strAttrs {
				if options.IncludeSyntheticAnnotations || !strings.HasPrefix(attr.Key, "$") {
					r.StringAttributes = append(r.StringAttributes, query.StringAnnotation{
						Key:   attr.Key,
						Value: attr.Value,
					})
				}
			}

			s.log.Info("fetching numerical attributes")
			numAttrRows, err := s.pool.Query(
				queryCtx,
				"SELECT key, value FROM numeric_attributes WHERE entity_key = $1 AND from_block = $2",
				keyHash.Bytes(),
				fromBlock,
			)
			if err != nil {
				return fmt.Errorf("error fetching numeric attributes: %w", err)
			}
			numAttrs, err := pgx.CollectRows(numAttrRows, pgx.RowToStructByName[query.NumericAnnotation])
			if err != nil {
				return fmt.Errorf("error fetching numeric attributes: %w", err)
			}

			// Convert numeric annotations
			for _, attr := range numAttrs {
				if options.IncludeSyntheticAnnotations || !strings.HasPrefix(attr.Key, "$") {
					r.NumericAttributes = append(r.NumericAttributes, query.NumericAnnotation{
						Key:   attr.Key,
						Value: uint64(attr.Value),
					})
				}
			}
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
