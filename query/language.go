package query

import (
	"cmp"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/Arkiv-Network/query-api/types"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

const AnnotationIdentRegex string = `[\p{L}_][\p{L}\p{N}_]*`

type Column struct {
	Name     string
	selector string
}

func (a Column) Compare(b Column) int {
	return cmp.Compare(a.Name, b.Name)
}

type OrderBy struct {
	Column     Column
	Descending bool
}

type QueryOptions struct {
	AtBlock            uint64
	IncludeData        *types.IncludeData
	Columns            []Column
	OrderBy            []OrderBy
	OrderByAnnotations []types.OrderByAnnotation
	Cursor             []types.CursorValue

	// Cache the sorted list of unique columns to fetch
	allColumnsSorted []string
	orderByColumns   []OrderBy

	Log *slog.Logger
}

func NewQueryOptions(log *slog.Logger, latestHead uint64, options *types.InternalQueryOptions) (*QueryOptions, error) {
	queryOptions := QueryOptions{
		Log:                log,
		OrderByAnnotations: options.OrderBy,
		IncludeData:        options.IncludeData,
	}

	queryOptions.Columns = []Column{}

	// We always need the primary key of the payloads table because of sorting
	queryOptions.Columns = append(queryOptions.Columns, Column{
		Name:     "from_block",
		selector: "e.from_block AS from_block",
	})
	queryOptions.Columns = append(queryOptions.Columns, Column{
		Name:     "entity_key",
		selector: "e.entity_key AS entity_key",
	})

	if options.IncludeData.Payload {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "payload",
			selector: "e.payload AS payload",
		})
	}
	if options.IncludeData.ContentType {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "content_type",
			selector: "e.content_type AS content_type",
		})
	}
	if options.IncludeData.Attributes {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "string_attributes",
			selector: "e.string_attributes AS string_attributes",
		})
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "numeric_attributes",
			selector: "e.numeric_attributes AS numeric_attributes",
		})
	}

	for i := range options.OrderBy {
		name := fmt.Sprintf("arkiv_annotation_sorting%d_value", i)
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name: name,
			selector: fmt.Sprintf(
				"arkiv_annotation_sorting%d.value AS arkiv_annotation_sorting%d_value",
				i, i,
			),
		})
	}

	if options.IncludeData.Owner {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "owner",
			selector: "ownerAttrs.Value AS owner",
		})
	}
	if options.IncludeData.Expiration {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "expires_at",
			selector: "expirationAttrs.Value AS expires_at",
		})
	}
	if options.IncludeData.CreatedAtBlock {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "created_at_block",
			selector: "createdAtBlockAttrs.Value AS created_at_block",
		})
	}
	// TODO derive these from the sequence
	if options.IncludeData.LastModifiedAtBlock ||
		options.IncludeData.TransactionIndexInBlock ||
		options.IncludeData.OperationIndexInTransaction {
		queryOptions.Columns = append(queryOptions.Columns, Column{
			Name:     "sequence",
			selector: "sequenceAttrs.Value AS sequence",
		})
	}

	// Sort so that we can use binary search later
	slices.SortFunc(queryOptions.Columns, Column.Compare)

	queryOptions.OrderBy = []OrderBy{}

	for i, o := range queryOptions.OrderByAnnotations {
		queryOptions.OrderBy = append(queryOptions.OrderBy, OrderBy{
			Column: Column{
				Name: fmt.Sprintf("arkiv_annotation_sorting%d_value", i),
				selector: fmt.Sprintf(
					"arkiv_annotation_sorting%d.value AS arkiv_annotation_sorting%d_value",
					i, i,
				),
			},
			Descending: o.Descending,
		})
	}
	queryOptions.OrderBy = append(queryOptions.OrderBy, OrderBy{
		Column: Column{
			Name:     "from_block",
			selector: "e.from_block",
		},
	})
	queryOptions.OrderBy = append(queryOptions.OrderBy, OrderBy{
		Column: Column{
			Name:     "entity_key",
			selector: "e.entity_key",
		},
	})

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

func (opts *QueryOptions) GetColumnIndex(column string) (int, error) {
	ix, found := slices.BinarySearchFunc(opts.Columns, column, func(a Column, b string) int {
		return cmp.Compare(a.Name, b)
	})

	if !found {
		return -1, fmt.Errorf("unknown column %s", column)
	}
	return ix, nil
}

func (opts *QueryOptions) EncodeCursor(cursor *types.Cursor) (string, error) {
	opts.Log.Info("encode cursor", "cursor", *cursor)
	encodedCursor := make([]any, 0, len(cursor.ColumnValues)*3+1)

	encodedCursor = append(encodedCursor, cursor.BlockNumber)

	for _, c := range cursor.ColumnValues {
		columnIx, err := opts.GetColumnIndex(c.ColumnName)
		if err != nil {
			return "", err
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

func (opts *QueryOptions) columnString() string {
	if len(opts.Columns) == 0 {
		return "1"
	}

	columns := make([]string, 0, len(opts.Columns))
	for _, c := range opts.Columns {
		columns = append(columns, c.selector)
	}

	return strings.Join(columns, ", ")
}

// Define the lexer with distinct tokens for each operator and parentheses.
var lex = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Whitespace", Pattern: `[ \t\n\r]+`},
	{Name: "LParen", Pattern: `\(`},
	{Name: "RParen", Pattern: `\)`},
	{Name: "And", Pattern: `&&`},
	{Name: "Or", Pattern: `\|\|`},
	{Name: "Neq", Pattern: `!=`},
	{Name: "Eq", Pattern: `=`},
	{Name: "Geqt", Pattern: `>=`},
	{Name: "Leqt", Pattern: `<=`},
	{Name: "Gt", Pattern: `>`},
	{Name: "Lt", Pattern: `<`},
	{Name: "NotGlob", Pattern: `!~`},
	{Name: "Glob", Pattern: `~`},
	{Name: "Not", Pattern: `!`},
	{Name: "EntityKey", Pattern: `0x[a-fA-F0-9]{64}`},
	{Name: "Address", Pattern: `0x[a-fA-F0-9]{40}`},
	{Name: "String", Pattern: `"(?:[^"\\]|\\.)*"`},
	{Name: "Number", Pattern: `[0-9]+`},
	{Name: "Ident", Pattern: AnnotationIdentRegex},
	// Meta-annotations, should start with $
	{Name: "Owner", Pattern: `\$owner`},
	{Name: "Creator", Pattern: `\$creator`},
	{Name: "Key", Pattern: `\$key`},
	{Name: "Expiration", Pattern: `\$expiration`},
	{Name: "Sequence", Pattern: `\$sequence`},
	{Name: "All", Pattern: `\$all`},
	{Name: "Star", Pattern: `\*`},
})

type SelectQuery struct {
	Query string
	Args  []any
}

type QueryBuilder struct {
	tableBuilder *strings.Builder
	args         []any
	argsCount    uint32
	tableCounter uint32
	needsComma   bool
	needsWhere   bool
	options      QueryOptions
}

func (b *QueryBuilder) nextTableName() string {
	b.tableCounter = b.tableCounter + 1
	return fmt.Sprintf("table_%d", b.tableCounter)
}

func (b *QueryBuilder) pushArgument(arg any) string {
	b.args = append(b.args, arg)
	b.argsCount += 1
	return fmt.Sprintf("$%d", b.argsCount)
}

func (b *QueryBuilder) writeComma() {
	if b.needsComma {
		b.tableBuilder.WriteString(", ")
	} else {
		b.needsComma = true
	}
}

func (b *QueryBuilder) addPaginationArguments() {
	paginationConditions := []string{}

	if len(b.options.Cursor) > 0 {
		for i := range b.options.Cursor {
			subcondition := []string{}
			for j, from := range b.options.Cursor {
				if j > i {
					break
				}
				var operator string
				if j < i {
					operator = "="
				} else if from.Descending {
					operator = "<"
				} else {
					operator = ">"
				}

				arg := b.pushArgument(from.Value)

				subcondition = append(
					subcondition,
					fmt.Sprintf("%s %s %s", from.ColumnName, operator, arg),
				)
			}

			paginationConditions = append(
				paginationConditions,
				fmt.Sprintf("(%s)", strings.Join(subcondition, " AND ")),
			)
		}

		paginationCondition := strings.Join(paginationConditions, " OR ")

		if b.needsWhere {
			b.tableBuilder.WriteString(" WHERE ")
			b.needsWhere = false
		} else {
			b.tableBuilder.WriteString(" AND ")
		}

		b.tableBuilder.WriteString(paginationCondition)
	}
}

func (b *QueryBuilder) createLeafQuery(query string) string {
	tableName := b.nextTableName()
	b.writeComma()
	b.tableBuilder.WriteString(tableName)
	b.tableBuilder.WriteString(" AS (")
	b.tableBuilder.WriteString(query)
	b.tableBuilder.WriteString(")")

	return tableName
}

type TopLevel struct {
	Expression *Expression `parser:"@@"`
	All        bool        `parser:"| @(All | Star)"`
}

func (t *TopLevel) Normalise() *TopLevel {
	if t.All {
		return t
	}
	return &TopLevel{
		Expression: t.Expression.Normalise(),
		All:        t.All,
	}
}

func (t *TopLevel) Evaluate(options *QueryOptions) (*SelectQuery, error) {
	tableBuilder := strings.Builder{}
	args := []any{}

	builder := QueryBuilder{
		options:      *options,
		tableBuilder: &tableBuilder,
		args:         args,
		needsComma:   false,
		needsWhere:   true,
	}

	if t.All {
		builder.tableBuilder.WriteString(strings.Join(
			[]string{
				"SELECT",
				builder.options.columnString(),
				"FROM payloads",
			},
			" ",
		))
	} else {
		builder.tableBuilder.WriteString(strings.Join(
			[]string{
				" SELECT",
				builder.options.columnString(),
				"FROM",
				t.Expression.Evaluate(&builder),
				"AS keys INNER JOIN payloads AS e ON keys.entity_key = e.entity_key AND keys.from_block = e.from_block",
			},
			" ",
		))
	}

	if builder.options.IncludeData.Owner {
		fmt.Fprintf(builder.tableBuilder,
			" LEFT JOIN string_attributes AS ownerAttrs"+
				" ON e.entity_key = ownerAttrs.entity_key"+
				" AND e.from_block = ownerAttrs.from_block"+
				" AND ownerAttrs.key = '%s'",
			types.OwnerAttributeKey,
		)
	}
	if builder.options.IncludeData.Expiration {
		fmt.Fprintf(builder.tableBuilder,
			" LEFT JOIN numeric_attributes AS expirationAttrs"+
				" ON e.entity_key = expirationAttrs.entity_key"+
				" AND e.from_block = expirationAttrs.from_block"+
				" AND expirationAttrs.key = '%s'",
			types.ExpirationAttributeKey,
		)
	}
	if builder.options.IncludeData.CreatedAtBlock {
		fmt.Fprintf(builder.tableBuilder,
			" LEFT JOIN numeric_attributes AS createdAtBlockAttrs"+
				" ON e.entity_key = createdAtBlockAttrs.entity_key"+
				" AND e.from_block = createdAtBlockAttrs.from_block"+
				" AND createdAtBlockAttrs.key = '%s'",
			types.CreatedAtBlockKey,
		)
	}
	if builder.options.IncludeData.LastModifiedAtBlock ||
		options.IncludeData.TransactionIndexInBlock ||
		options.IncludeData.OperationIndexInTransaction {
		fmt.Fprintf(builder.tableBuilder,
			" LEFT JOIN numeric_attributes AS sequenceAttrs"+
				" ON e.entity_key = sequenceAttrs.entity_key"+
				" AND e.from_block = sequenceAttrs.from_block"+
				" AND sequenceAttrs.key = '%s'",
			types.SequenceAttributeKey,
		)
	}

	for i, orderBy := range builder.options.OrderByAnnotations {
		tableName := ""
		switch orderBy.Type {
		case "string":
			tableName = "string_attributes"
		case "numeric":
			tableName = "numeric_attributes"
		default:
			return nil, fmt.Errorf("a type of either 'string' or 'numeric' needs to be provided for the annotation '%s'", orderBy.Name)
		}

		sortingTable := fmt.Sprintf("arkiv_annotation_sorting%d", i)

		keyPlaceholder := builder.pushArgument(orderBy.Name)

		fmt.Fprintf(builder.tableBuilder,
			" LEFT JOIN %s AS %s"+
				" ON %s.entity_key = e.entity_key"+
				" AND %s.from_block = e.from_block"+
				" AND %s.key = %s",

			tableName,
			sortingTable,
			sortingTable,
			sortingTable,
			sortingTable,
			keyPlaceholder,
		)
	}

	builder.addPaginationArguments()

	if builder.needsWhere {
		builder.tableBuilder.WriteString(" WHERE ")
		builder.needsWhere = false
	} else {
		builder.tableBuilder.WriteString(" AND ")
	}

	blockArg := builder.pushArgument(builder.options.AtBlock)
	fmt.Fprintf(builder.tableBuilder, "%s BETWEEN e.from_block AND e.to_block", blockArg)

	builder.tableBuilder.WriteString(" ORDER BY ")

	orderColumns := make([]string, 0, len(builder.options.OrderBy))
	for _, o := range builder.options.OrderBy {
		suffix := ""
		if o.Descending {
			suffix = " DESC"
		}
		orderColumns = append(orderColumns, o.Column.Name+suffix)
	}
	builder.tableBuilder.WriteString(strings.Join(orderColumns, ", "))

	builder.tableBuilder.WriteString(" LIMIT 1000")

	return &SelectQuery{
		Query: builder.tableBuilder.String(),
		Args:  builder.args,
	}, nil
}

// Expression is the top-level rule.
type Expression struct {
	Or OrExpression `parser:"@@"`
}

func (e *Expression) Normalise() *Expression {
	normalised := e.Or.Normalise()
	// Remove unneeded OR+AND nodes that both only contain a single child
	// when that child is a parenthesised expression
	if len(normalised.Right) == 0 && len(normalised.Left.Right) == 0 && normalised.Left.Left.Paren != nil {
		// This has already been normalised by the call above, so any negation has
		// been pushed into the leaf expressions and we can safely strip away the
		// parentheses
		return &normalised.Left.Left.Paren.Nested
	}
	return &Expression{
		Or: *normalised,
	}
}

func (e *Expression) invert() *Expression {

	newLeft := e.Or.invert()

	if len(newLeft.Right) == 0 {
		// By construction, this will always be a Paren
		if newLeft.Left.Paren == nil {
			panic("This should never happen!")
		}
		return &newLeft.Left.Paren.Nested
	}

	return &Expression{
		Or: OrExpression{
			Left: *newLeft,
		},
	}
}

func (e *Expression) Evaluate(builder *QueryBuilder) string {
	builder.tableBuilder.WriteString("WITH ")
	prevTable := e.Or.Evaluate(builder)

	builder.writeComma()
	nextTable := builder.nextTableName()

	builder.tableBuilder.WriteString(nextTable)
	builder.tableBuilder.WriteString(" AS (")
	builder.tableBuilder.WriteString("SELECT DISTINCT * FROM ")
	builder.tableBuilder.WriteString(prevTable)
	builder.tableBuilder.WriteString(")")

	return nextTable
}

// OrExpression handles expressions connected with ||.
type OrExpression struct {
	Left  AndExpression `parser:"@@"`
	Right []*OrRHS      `parser:"@@*"`
}

func (e *OrExpression) Normalise() *OrExpression {
	var newRight []*OrRHS = nil

	if e.Right != nil {
		newRight = make([]*OrRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.Normalise())
		}
	}

	return &OrExpression{
		Left:  *e.Left.Normalise(),
		Right: newRight,
	}
}

func (e *OrExpression) invert() *AndExpression {
	newLeft := EqualExpr{
		Paren: &Paren{
			IsNot: false,
			Nested: Expression{
				Or: *e.Left.invert(),
			},
		},
	}

	var newRight []*AndRHS = nil

	if e.Right != nil {
		newRight = make([]*AndRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &AndExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

func (e *OrExpression) Evaluate(b *QueryBuilder) string {
	leftTable := e.Left.Evaluate(b)
	tableName := leftTable

	for _, rhs := range e.Right {
		rightTable := rhs.Evaluate(b)
		tableName = b.nextTableName()

		b.writeComma()

		b.tableBuilder.WriteString(tableName)
		b.tableBuilder.WriteString(" AS (")
		b.tableBuilder.WriteString("SELECT * FROM ")
		b.tableBuilder.WriteString(leftTable)
		b.tableBuilder.WriteString(" UNION ")
		b.tableBuilder.WriteString("SELECT * FROM ")
		b.tableBuilder.WriteString(rightTable)
		b.tableBuilder.WriteString(")")

		// Carry forward the cumulative result of the UNION
		leftTable = tableName
	}

	return tableName
}

// OrRHS represents the right-hand side of an OR.
type OrRHS struct {
	Expr AndExpression `parser:"(Or | 'OR' | 'or') @@"`
}

func (e *OrRHS) Normalise() *OrRHS {
	return &OrRHS{
		Expr: *e.Expr.Normalise(),
	}
}

func (e *OrRHS) invert() *AndRHS {
	return &AndRHS{
		Expr: EqualExpr{
			Paren: &Paren{
				IsNot: false,
				Nested: Expression{
					Or: *e.Expr.invert(),
				},
			},
		},
	}
}

func (e *OrRHS) Evaluate(b *QueryBuilder) string {
	return e.Expr.Evaluate(b)
}

// AndExpression handles expressions connected with &&.
type AndExpression struct {
	Left  EqualExpr `parser:"@@"`
	Right []*AndRHS `parser:"@@*"`
}

func (e *AndExpression) Normalise() *AndExpression {
	var newRight []*AndRHS = nil

	if e.Right != nil {
		newRight = make([]*AndRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.Normalise())
		}
	}

	return &AndExpression{
		Left:  *e.Left.Normalise(),
		Right: newRight,
	}
}

func (e *AndExpression) invert() *OrExpression {
	newLeft := AndExpression{
		Left: *e.Left.invert(),
	}

	var newRight []*OrRHS = nil

	if e.Right != nil {
		newRight = make([]*OrRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &OrExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

func (e *AndExpression) Evaluate(b *QueryBuilder) string {
	leftTable := e.Left.Evaluate(b)
	tableName := leftTable

	for _, rhs := range e.Right {
		rightTable := rhs.Evaluate(b)
		tableName = b.nextTableName()

		b.writeComma()

		b.tableBuilder.WriteString(tableName)
		b.tableBuilder.WriteString(" AS (")
		b.tableBuilder.WriteString("SELECT * FROM ")
		b.tableBuilder.WriteString(leftTable)
		b.tableBuilder.WriteString(" INTERSECT ")
		b.tableBuilder.WriteString("SELECT * FROM ")
		b.tableBuilder.WriteString(rightTable)
		b.tableBuilder.WriteString(")")

		// Carry forward the cumulative result of the INTERSECT
		leftTable = tableName
	}

	return tableName
}

// AndRHS represents the right-hand side of an AND.
type AndRHS struct {
	Expr EqualExpr `parser:"(And | 'AND' | 'and') @@"`
}

func (e *AndRHS) Normalise() *AndRHS {
	return &AndRHS{
		Expr: *e.Expr.Normalise(),
	}
}

func (e *AndRHS) invert() *OrRHS {
	return &OrRHS{
		Expr: AndExpression{
			Left: *e.Expr.invert(),
		},
	}
}

func (e *AndRHS) Evaluate(b *QueryBuilder) string {
	return e.Expr.Evaluate(b)
}

// EqualExpr can be either an equality or a parenthesized expression.
type EqualExpr struct {
	Paren     *Paren     `parser:"  @@"`
	Assign    *Equality  `parser:"| @@"`
	Inclusion *Inclusion `parser:"| @@"`

	LessThan           *LessThan           `parser:"| @@"`
	LessOrEqualThan    *LessOrEqualThan    `parser:"| @@"`
	GreaterThan        *GreaterThan        `parser:"| @@"`
	GreaterOrEqualThan *GreaterOrEqualThan `parser:"| @@"`
	Glob               *Glob               `parser:"| @@"`
}

func (e *EqualExpr) Normalise() *EqualExpr {
	normalised := e

	if e.Paren != nil {
		p := e.Paren.Normalise()

		// Remove parentheses that only contain a single nested expression
		// (i.e. no OR or AND with multiple children)
		if len(p.Nested.Or.Right) == 0 && len(p.Nested.Or.Left.Right) == 0 {
			// This expression should already be properly normalised, we don't need to
			// call Normalise again here
			normalised = &p.Nested.Or.Left.Left
		} else {
			normalised = &EqualExpr{Paren: p}
		}
	}

	// Everything other than parenthesised expressions do not require further normalisation
	return normalised
}

func (e *EqualExpr) invert() *EqualExpr {
	if e.Paren != nil {
		return &EqualExpr{Paren: e.Paren.invert()}
	}

	if e.LessThan != nil {
		return &EqualExpr{GreaterOrEqualThan: e.LessThan.invert()}
	}

	if e.LessOrEqualThan != nil {
		return &EqualExpr{GreaterThan: e.LessOrEqualThan.invert()}
	}

	if e.GreaterThan != nil {
		return &EqualExpr{LessOrEqualThan: e.GreaterThan.invert()}
	}

	if e.GreaterOrEqualThan != nil {
		return &EqualExpr{LessThan: e.GreaterOrEqualThan.invert()}
	}

	if e.Glob != nil {
		return &EqualExpr{Glob: e.Glob.invert()}
	}

	if e.Assign != nil {
		return &EqualExpr{Assign: e.Assign.invert()}
	}

	if e.Inclusion != nil {
		return &EqualExpr{Inclusion: e.Inclusion.invert()}
	}

	panic("This should not happen!")
}

func (e *EqualExpr) Evaluate(b *QueryBuilder) string {
	if e.Paren != nil {
		return e.Paren.Evaluate(b)
	}

	if e.LessThan != nil {
		return e.LessThan.Evaluate(b)
	}

	if e.LessOrEqualThan != nil {
		return e.LessOrEqualThan.Evaluate(b)
	}

	if e.GreaterThan != nil {
		return e.GreaterThan.Evaluate(b)
	}

	if e.GreaterOrEqualThan != nil {
		return e.GreaterOrEqualThan.Evaluate(b)
	}

	if e.Glob != nil {
		return e.Glob.Evaluate(b)
	}

	if e.Assign != nil {
		return e.Assign.Evaluate(b)
	}

	if e.Inclusion != nil {
		return e.Inclusion.Evaluate(b)
	}

	panic("This should not happen!")
}

type Paren struct {
	IsNot  bool       `parser:"@(Not | 'NOT' | 'not')?"`
	Nested Expression `parser:"LParen @@ RParen"`
}

func (e *Paren) Normalise() *Paren {
	nested := e.Nested

	if e.IsNot {
		nested = *nested.invert()
	}

	return &Paren{
		IsNot:  false,
		Nested: *nested.Normalise(),
	}
}

func (e *Paren) invert() *Paren {
	return &Paren{
		IsNot:  !e.IsNot,
		Nested: e.Nested,
	}
}

func (e *Paren) Evaluate(b *QueryBuilder) string {
	expr := e.Nested
	// If we have a negation, we will push it down into the expression
	if e.IsNot {
		expr = *e.Nested.invert()
	}
	// We don't have to do anything here regarding precedence, the parsing order
	// is already taking care of precedence since the nested OR node will create a subquery
	return expr.Or.Evaluate(b)
}

func (b *QueryBuilder) createAnnotationQuery(
	attributeType string,
	whereClause string,
) string {

	tableName := "string_attributes"
	if attributeType == "numeric" {
		tableName = "numeric_attributes"
	}

	blockArg := b.pushArgument(b.options.AtBlock)

	return b.createLeafQuery(
		strings.Join(
			[]string{
				"SELECT e.entity_key, e.from_block FROM",
				tableName,
				"AS a",
				"INNER JOIN payloads AS e",
				"ON a.entity_key = e.entity_key",
				"AND a.from_block = e.from_block",
				fmt.Sprintf("AND %s BETWEEN e.from_block AND e.to_block", blockArg),
				"WHERE",
				whereClause,
			},
			" ",
		),
	)
}

type Glob struct {
	Var   string `parser:"@Ident"`
	IsNot bool   `parser:"((Glob | @NotGlob) | (@('NOT' | 'not')? ('GLOB' | 'glob')))"`
	Value string `parser:"@String"`
}

func (e *Glob) invert() *Glob {
	return &Glob{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

func (e *Glob) Evaluate(b *QueryBuilder) string {
	varArg := b.pushArgument(e.Var)
	valArg := b.pushArgument(e.Value)

	op := "~"
	if e.IsNot {
		op = "!~"
	}

	return b.createAnnotationQuery(
		"string",
		fmt.Sprintf("key = %s AND value %s %s", varArg, op, valArg),
	)
}

type LessThan struct {
	Var   string `parser:"@Ident Lt"`
	Value Value  `parser:"@@"`
}

func (e *LessThan) invert() *GreaterOrEqualThan {
	return &GreaterOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *LessThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("key = %s AND value < %s", varArg, valArg),
	)
}

type LessOrEqualThan struct {
	Var   string `parser:"@Ident Leqt"`
	Value Value  `parser:"@@"`
}

func (e *LessOrEqualThan) invert() *GreaterThan {
	return &GreaterThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *LessOrEqualThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("key = %s AND value <= %s", varArg, valArg),
	)
}

type GreaterThan struct {
	Var   string `parser:"@Ident Gt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterThan) invert() *LessOrEqualThan {
	return &LessOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *GreaterThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("key = %s AND value > %s", varArg, valArg),
	)
}

type GreaterOrEqualThan struct {
	Var   string `parser:"@Ident Geqt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterOrEqualThan) invert() *LessThan {
	return &LessThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

func (e *GreaterOrEqualThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("key = %s AND value >= %s", varArg, valArg),
	)
}

// Equality represents a simple equality (e.g. name = 123).
type Equality struct {
	Var   string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot bool   `parser:"(Eq | @Neq)"`
	Value Value  `parser:"@@"`
}

func (e *Equality) invert() *Equality {
	return &Equality{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

func (e *Equality) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	op := "="
	if e.IsNot {
		op = "!="
	}

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("key = %s AND value %s %s", varArg, op, valArg),
	)
}

type Inclusion struct {
	Var    string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot  bool   `parser:"(@('NOT'|'not')? ('IN'|'in'))"`
	Values Values `parser:"@@"`
}

func (e *Inclusion) invert() *Inclusion {
	return &Inclusion{
		Var:    e.Var,
		IsNot:  !e.IsNot,
		Values: e.Values,
	}
}

func (e *Inclusion) Evaluate(b *QueryBuilder) string {
	var values []string
	attrType := "string"
	if len(e.Values.Strings) > 0 {

		values = make([]string, 0, len(e.Values.Strings))
		for _, value := range e.Values.Strings {
			if e.Var == types.OwnerAttributeKey ||
				e.Var == types.CreatorAttributeKey ||
				e.Var == types.KeyAttributeKey {
				values = append(values, b.pushArgument(strings.ToLower(value)))
			} else {
				values = append(values, b.pushArgument(value))
			}
		}

	} else {
		attrType = "numeric"
		values = make([]string, 0, len(e.Values.Numbers)+1)
		values = append(values, e.Var)
		for _, value := range e.Values.Numbers {
			values = append(values, b.pushArgument(value))
		}
	}

	paramStr := strings.Join(values, ", ")

	condition := fmt.Sprintf("a.value IN (%s)", paramStr)
	if e.IsNot {
		condition = fmt.Sprintf("a.value NOT IN (%s)", paramStr)
	}

	keyArg := b.pushArgument(e.Var)

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND %s", keyArg,
			condition,
		),
	)
}

// Value is a literal value (a number or a string).
type Value struct {
	String *string `parser:"  (@String | @EntityKey | @Address)"`
	Number *uint64 `parser:"| @Number"`
}

type Values struct {
	Strings []string `parser:"  '(' (@String | @EntityKey | @Address)+ ')'"`
	Numbers []uint64 `parser:"| '(' @Number+ ')'"`
}

var Parser = participle.MustBuild[TopLevel](
	participle.Lexer(lex),
	participle.Elide("Whitespace"),
	participle.Unquote("String"),
)

func Parse(s string, log *slog.Logger) (*TopLevel, error) {
	log.Info("parsing query", "query", s)

	v, err := Parser.ParseString("", s)
	if err != nil {
		return nil, err
	}
	return v.Normalise(), err
}
