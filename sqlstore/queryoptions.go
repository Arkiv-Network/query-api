package sqlstore

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/Arkiv-Network/sqlite-store/query"
)

func NewQueryOptions(log *slog.Logger, latestHead uint64, options *query.InternalQueryOptions) (*query.QueryOptions, error) {
	queryOptions := query.QueryOptions{
		Log:                log,
		OrderByAnnotations: options.OrderBy,
		IncludeData:        options.IncludeData,
	}

	queryOptions.Columns = []query.Column{}

	// We always need the primary key of the payloads table because of sorting
	queryOptions.Columns = append(queryOptions.Columns,
		query.Column{
			Name:          "from_block",
			QualifiedName: "e.from_block",
		},
		query.Column{
			Name:          "entity_key",
			QualifiedName: "e.entity_key",
			IsBytes:       true,
		},
	)

	if options.IncludeData.Payload {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "payload",
			QualifiedName: "e.payload",
		})
	}
	if options.IncludeData.ContentType {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "content_type",
			QualifiedName: "e.content_type",
		})
	}
	if options.IncludeData.Attributes {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "string_attributes",
			QualifiedName: "e.string_attributes",
		})
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "numeric_attributes",
			QualifiedName: "e.numeric_attributes",
		})
	}

	for i := range options.OrderBy {
		name := fmt.Sprintf("arkiv_annotation_sorting%d_value", i)
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          name,
			QualifiedName: fmt.Sprintf("arkiv_annotation_sorting%d.value", i),
		})
	}

	if options.IncludeData.Owner {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "owner",
			QualifiedName: fmt.Sprintf("e.string_attributes ->> '%s'", query.OwnerAttributeKey),
		})
	}
	if options.IncludeData.Expiration {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "expires_at",
			QualifiedName: fmt.Sprintf("e.numeric_attributes ->> '%s'", query.ExpirationAttributeKey),
		})
	}
	if options.IncludeData.CreatedAtBlock {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "created_at_block",
			QualifiedName: fmt.Sprintf("e.numeric_attributes ->> '%s'", query.CreatedAtBlockKey),
		})
	}
	if options.IncludeData.LastModifiedAtBlock ||
		options.IncludeData.TransactionIndexInBlock ||
		options.IncludeData.OperationIndexInTransaction {
		queryOptions.Columns = append(queryOptions.Columns, query.Column{
			Name:          "sequence",
			QualifiedName: fmt.Sprintf("e.numeric_attributes ->> '%s'", query.SequenceAttributeKey),
		})
	}

	// Sort so that we can use binary search later
	slices.SortFunc(queryOptions.Columns, query.Column.Compare)

	queryOptions.OrderBy = []query.OrderBy{}

	for i, o := range queryOptions.OrderByAnnotations {
		queryOptions.OrderBy = append(queryOptions.OrderBy, query.OrderBy{
			Column: query.Column{
				Name:          fmt.Sprintf("arkiv_annotation_sorting%d_value", i),
				QualifiedName: fmt.Sprintf("arkiv_annotation_sorting%d.value", i),
			},
			Descending: o.Descending,
		})
	}
	queryOptions.OrderBy = append(queryOptions.OrderBy,
		query.OrderBy{
			Column: query.Column{
				Name:          "from_block",
				QualifiedName: "e.from_block",
			},
		},
		query.OrderBy{
			Column: query.Column{
				Name:          "entity_key",
				QualifiedName: "e.entity_key",
				IsBytes:       true,
			},
		},
	)

	queryOptions.AtBlock = latestHead

	if len(options.Cursor) != 0 {
		cursor, err := queryOptions.DecodeCursor(options.Cursor)
		if err != nil {
			return nil, err
		}
		queryOptions.AtBlock = cursor.BlockNumber
		queryOptions.Cursor = cursor.ColumnValues
	}

	if options.AtBlock != nil {
		queryOptions.AtBlock = *options.AtBlock
	}

	return &queryOptions, nil
}
