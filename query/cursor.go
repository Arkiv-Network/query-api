package query

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Arkiv-Network/query-api/types"
)

func (opts *QueryOptions) EncodeCursor(cursor *types.Cursor) (string, error) {
	bs, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("error marshalling cursor: %w", err)
	}
	opts.Log.Info("encode cursor", "cursor", string(bs))
	encodedCursor := make([]any, 0, len(cursor.ColumnValues)*3+1)

	encodedCursor = append(encodedCursor, cursor.BlockNumber)

	for _, c := range cursor.ColumnValues {
		columnIx, err := opts.GetColumnIndex(c.ColumnName)
		if err != nil {
			return "", fmt.Errorf("could not find column index: %w", err)
		}
		descending := uint64(0)
		if c.Descending {
			descending = 1
		}
		encodedCursor = append(encodedCursor,
			uint64(columnIx), c.Value, descending,
		)
	}

	s, err := json.Marshal(encodedCursor)
	if err != nil {
		return "", fmt.Errorf("could not marshal cursor: %w", err)
	}
	opts.Log.Info("Encoded cursor", "cursor", string(s))

	hexCursor := hex.EncodeToString([]byte(s))
	opts.Log.Info("Hex encoded cursor", "cursor", hexCursor)

	return hexCursor, nil
}

func (opts *QueryOptions) DecodeCursor(cursorStr string) (*types.Cursor, error) {
	if len(cursorStr) == 0 {
		return nil, nil
	}

	bs, err := hex.DecodeString(cursorStr)
	if err != nil {
		return nil, fmt.Errorf("could not decode cursor: %w", err)
	}

	cursor := types.Cursor{}

	encoded := make([]any, 0)
	err = json.Unmarshal(bs, &encoded)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal cursor: %w (%s)", err, string(bs))
	}

	firstValue, ok := encoded[0].(float64)
	if !ok {
		return nil, fmt.Errorf("invalid block number: %d", encoded[0])
	}
	blockNumber := uint64(firstValue)
	cursor.BlockNumber = blockNumber

	cursor.ColumnValues = make([]types.CursorValue, 0, len(encoded)-1)

	for c := range slices.Chunk(encoded[1:], 3) {
		if len(c) != 3 {
			return nil, fmt.Errorf("invalid length of cursor array: %d", len(c))
		}

		firstValue, ok := c[0].(float64)
		if !ok {
			return nil, fmt.Errorf("unknown column index: %d", c[0])
		}
		thirdValue, ok := c[2].(float64)
		if !ok {
			return nil, fmt.Errorf("unknown value for descending: %d", c[3])
		}

		columnIx := int(firstValue)
		if columnIx >= len(opts.Columns) {
			return nil, fmt.Errorf("unknown column index: %d", columnIx)
		}

		descendingInt := int(thirdValue)
		descending := false
		switch descendingInt {
		case 0:
			descending = false
		case 1:
			descending = true
		default:
			return nil, fmt.Errorf("unknown value for descending: %d", descendingInt)
		}

		cursor.ColumnValues = append(cursor.ColumnValues, types.CursorValue{
			ColumnName: opts.Columns[columnIx].Name,
			Value:      c[1],
			Descending: descending,
		})
	}

	jsonCursor, err := json.Marshal(cursor)
	if err != nil {
		return nil, err
	}
	opts.Log.Info("Decoded cursor", "cursor", string(jsonCursor))

	return &cursor, nil
}
