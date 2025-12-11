package query

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var KeyAttributeKey = "$key"
var CreatorAttributeKey = "$creator"
var OwnerAttributeKey = "$owner"
var ExpirationAttributeKey = "$expiration"
var CreatedAtBlockKey = "$createdAtBlock"
var SequenceAttributeKey = "$sequence"

type OrderByAnnotation struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Descending bool   `json:"desc"`
}

type QueryResponse struct {
	Data        []json.RawMessage `json:"data"`
	BlockNumber uint64            `json:"blockNumber"`
	Cursor      *string           `json:"cursor,omitempty"`
}

type Cursor struct {
	BlockNumber  uint64        `json:"blockNumber"`
	ColumnValues []CursorValue `json:"columnValues"`
}

type CursorValue struct {
	ColumnName string `json:"columnName"`
	Value      any    `json:"value"`
	Descending bool   `json:"desc"`
}

type StringAnnotation struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type NumericAnnotation struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

type EntityData struct {
	Key                         *common.Hash    `json:"key,omitempty"`
	Value                       hexutil.Bytes   `json:"value,omitempty"`
	ContentType                 *string         `json:"contentType,omitempty"`
	ExpiresAt                   *uint64         `json:"expiresAt,omitempty"`
	Owner                       *common.Address `json:"owner,omitempty"`
	CreatedAtBlock              *uint64         `json:"createdAtBlock,omitempty"`
	LastModifiedAtBlock         *uint64         `json:"lastModifiedAtBlock,omitempty"`
	TransactionIndexInBlock     *uint64         `json:"transactionIndexInBlock,omitempty"`
	OperationIndexInTransaction *uint64         `json:"operationIndexInTransaction,omitempty"`

	StringAttributes  []StringAnnotation  `json:"stringAttributes,omitempty"`
	NumericAttributes []NumericAnnotation `json:"numericAttributes,omitempty"`
}

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

type Options struct {
	AtBlock        *uint64             `json:"atBlock"`
	IncludeData    *IncludeData        `json:"includeData"`
	OrderBy        []OrderByAnnotation `json:"orderBy"`
	ResultsPerPage uint64              `json:"resultsPerPage"`
	Cursor         string              `json:"cursor"`
}

func (options *Options) ToInternalQueryOptions() (*InternalQueryOptions, error) {
	defaultIncludeData := &IncludeData{
		Key:         true,
		Expiration:  true,
		Owner:       true,
		Payload:     true,
		ContentType: true,
		Attributes:  true,
	}
	switch {
	case options == nil:
		return &InternalQueryOptions{
			IncludeData: defaultIncludeData,
		}, nil
	case options.IncludeData == nil:
		return &InternalQueryOptions{
			IncludeData: defaultIncludeData,
			OrderBy:     options.OrderBy,
			AtBlock:     options.AtBlock,
			Cursor:      options.Cursor,
		}, nil
	default:
		iq := InternalQueryOptions{
			OrderBy:     options.OrderBy,
			AtBlock:     options.AtBlock,
			Cursor:      options.Cursor,
			IncludeData: options.IncludeData,
		}
		return &iq, nil
	}
}

type InternalQueryOptions struct {
	AtBlock     *uint64             `json:"atBlock"`
	IncludeData *IncludeData        `json:"includeData"`
	OrderBy     []OrderByAnnotation `json:"orderBy"`
	Cursor      string              `json:"cursor"`
}
