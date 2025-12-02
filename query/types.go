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
