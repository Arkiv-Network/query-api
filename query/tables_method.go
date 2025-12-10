package query

import (
	"fmt"
	"strings"
)

func (b *QueryBuilder) createLeafQuery(query string) string {
	tableName := b.nextTableName()
	b.writeComma()
	b.queryBuilder.WriteString(tableName)
	b.queryBuilder.WriteString(" AS (")
	b.queryBuilder.WriteString(query)
	b.queryBuilder.WriteString(")")

	return tableName
}

func (t *TopLevel) Evaluate(options *QueryOptions) (*SelectQuery, error) {
	tableBuilder := strings.Builder{}
	args := []any{}

	builder := QueryBuilder{
		options:      *options,
		queryBuilder: &tableBuilder,
		args:         args,
		needsComma:   false,
		needsWhere:   true,
	}

	if t.Expression != nil {
		builder.queryBuilder.WriteString(strings.Join(
			[]string{
				" SELECT",
				builder.options.columnString(),
				"FROM",
				t.Expression.Evaluate(&builder),
				"AS keys INNER JOIN payloads AS e ON keys.entity_key = e.entity_key AND keys.from_block = e.from_block",
			},
			" ",
		))
	} else {
		builder.queryBuilder.WriteString(strings.Join(
			[]string{
				"SELECT",
				builder.options.columnString(),
				"FROM payloads AS e",
			},
			" ",
		))
	}

	if builder.options.IncludeData != nil {
		if builder.options.IncludeData.Owner {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN string_attributes AS ownerAttrs"+
					" ON e.entity_key = ownerAttrs.entity_key"+
					" AND e.from_block = ownerAttrs.from_block"+
					" AND ownerAttrs.key = '%s'",
				OwnerAttributeKey,
			)
		}
		if builder.options.IncludeData.Expiration {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS expirationAttrs"+
					" ON e.entity_key = expirationAttrs.entity_key"+
					" AND e.from_block = expirationAttrs.from_block"+
					" AND expirationAttrs.key = '%s'",
				ExpirationAttributeKey,
			)
		}
		if builder.options.IncludeData.CreatedAtBlock {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS createdAtBlockAttrs"+
					" ON e.entity_key = createdAtBlockAttrs.entity_key"+
					" AND e.from_block = createdAtBlockAttrs.from_block"+
					" AND createdAtBlockAttrs.key = '%s'",
				CreatedAtBlockKey,
			)
		}
		if builder.options.IncludeData.LastModifiedAtBlock ||
			options.IncludeData.TransactionIndexInBlock ||
			options.IncludeData.OperationIndexInTransaction {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS sequenceAttrs"+
					" ON e.entity_key = sequenceAttrs.entity_key"+
					" AND e.from_block = sequenceAttrs.from_block"+
					" AND sequenceAttrs.key = '%s'",
				SequenceAttributeKey,
			)
		}
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

		fmt.Fprintf(builder.queryBuilder,
			" LEFT JOIN %[1]s AS %s"+
				" ON %[2]s.entity_key = e.entity_key"+
				" AND %[2]s.from_block = e.from_block"+
				" AND %[2]s.key = %[3]s",

			tableName,
			sortingTable,
			keyPlaceholder,
		)
	}

	err := builder.addPaginationArguments()
	if err != nil {
		return nil, fmt.Errorf("error adding the pagination condition: %w", err)
	}

	if builder.needsWhere {
		builder.queryBuilder.WriteString(" WHERE ")
		builder.needsWhere = false
	} else {
		builder.queryBuilder.WriteString(" AND ")
	}

	blockArg := builder.pushArgument(builder.options.AtBlock)
	fmt.Fprintf(builder.queryBuilder, "%s BETWEEN e.from_block AND e.to_block", blockArg)

	builder.queryBuilder.WriteString(" ORDER BY ")

	orderColumns := make([]string, 0, len(builder.options.OrderBy))
	for _, o := range builder.options.OrderBy {
		suffix := ""
		if o.Descending {
			suffix = " DESC"
		}
		orderColumns = append(orderColumns, o.Column.Name+suffix)
	}
	builder.queryBuilder.WriteString(strings.Join(orderColumns, ", "))

	fmt.Fprintf(builder.queryBuilder, " LIMIT %d", QueryResultCountLimit)

	return &SelectQuery{
		Query: builder.queryBuilder.String(),
		Args:  builder.args,
	}, nil
}

func (e *Expression) Evaluate(builder *QueryBuilder) string {
	builder.queryBuilder.WriteString("WITH ")
	prevTable := e.Or.Evaluate(builder)

	builder.writeComma()
	nextTable := builder.nextTableName()

	builder.queryBuilder.WriteString(nextTable)
	builder.queryBuilder.WriteString(" AS (")
	builder.queryBuilder.WriteString("SELECT DISTINCT * FROM ")
	builder.queryBuilder.WriteString(prevTable)
	builder.queryBuilder.WriteString(")")

	return nextTable
}

func (e *OrExpression) Evaluate(b *QueryBuilder) string {
	leftTable := e.Left.Evaluate(b)
	tableName := leftTable

	for _, rhs := range e.Right {
		rightTable := rhs.Evaluate(b)
		tableName = b.nextTableName()

		b.writeComma()

		b.queryBuilder.WriteString(tableName)
		b.queryBuilder.WriteString(" AS (")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(leftTable)
		b.queryBuilder.WriteString(" UNION ")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(rightTable)
		b.queryBuilder.WriteString(")")

		// Carry forward the cumulative result of the UNION
		leftTable = tableName
	}

	return tableName
}

func (e *OrRHS) Evaluate(b *QueryBuilder) string {
	return e.Expr.Evaluate(b)
}

func (e *AndExpression) Evaluate(b *QueryBuilder) string {
	leftTable := e.Left.Evaluate(b)
	tableName := leftTable

	for _, rhs := range e.Right {
		rightTable := rhs.Evaluate(b)
		tableName = b.nextTableName()

		b.writeComma()

		b.queryBuilder.WriteString(tableName)
		b.queryBuilder.WriteString(" AS (")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(leftTable)
		b.queryBuilder.WriteString(" INTERSECT ")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(rightTable)
		b.queryBuilder.WriteString(")")

		// Carry forward the cumulative result of the INTERSECT
		leftTable = tableName
	}

	return tableName
}

func (e *AndRHS) Evaluate(b *QueryBuilder) string {
	return e.Expr.Evaluate(b)
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

func (e *Inclusion) Evaluate(b *QueryBuilder) string {
	var values []string
	attrType := "string"
	if len(e.Values.Strings) > 0 {

		values = make([]string, 0, len(e.Values.Strings))
		for _, value := range e.Values.Strings {
			if e.Var == OwnerAttributeKey ||
				e.Var == CreatorAttributeKey ||
				e.Var == KeyAttributeKey {
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
