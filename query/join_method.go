package query

import (
	"fmt"
	"strings"
)

type AttrJoin struct {
	Table string
	Alias string
}

func (t *TopLevel) Evaluate2(options *QueryOptions) (*SelectQuery, error) {
	qb := strings.Builder{}
	args := []any{}

	b := QueryBuilder{
		options:      *options,
		queryBuilder: &qb,
		args:         args,
		needsComma:   false,
		needsWhere:   true,
	}

	b.queryBuilder.WriteString(strings.Join(
		[]string{
			"SELECT",
			b.options.columnString(),
			"FROM payloads AS e",
		},
		" ",
	))

	if t.Expression != nil {
		attrJoins := make(map[string]AttrJoin)
		t.Expression.addJoins(attrJoins)

		for attrName, join := range attrJoins {
			attrPlaceholder := b.pushArgument(attrName)
			fmt.Fprintf(
				b.queryBuilder,
				" INNER JOIN %[1]s AS %[2]s ON e.entity_key = %[2]s.entity_key AND e.from_block = %[2]s.from_block AND %[2]s.key = %[3]s",
				join.Table,
				join.Alias,
				attrPlaceholder,
			)
		}
	}

	if b.options.IncludeData.Owner {
		fmt.Fprintf(b.queryBuilder,
			" LEFT JOIN string_attributes AS ownerAttrs"+
				" ON e.entity_key = ownerAttrs.entity_key"+
				" AND e.from_block = ownerAttrs.from_block"+
				" AND ownerAttrs.key = '%s'",
			OwnerAttributeKey,
		)
	}
	if b.options.IncludeData.Expiration {
		fmt.Fprintf(b.queryBuilder,
			" LEFT JOIN numeric_attributes AS expirationAttrs"+
				" ON e.entity_key = expirationAttrs.entity_key"+
				" AND e.from_block = expirationAttrs.from_block"+
				" AND expirationAttrs.key = '%s'",
			ExpirationAttributeKey,
		)
	}
	if b.options.IncludeData.CreatedAtBlock {
		fmt.Fprintf(b.queryBuilder,
			" LEFT JOIN numeric_attributes AS createdAtBlockAttrs"+
				" ON e.entity_key = createdAtBlockAttrs.entity_key"+
				" AND e.from_block = createdAtBlockAttrs.from_block"+
				" AND createdAtBlockAttrs.key = '%s'",
			CreatedAtBlockKey,
		)
	}
	if b.options.IncludeData.LastModifiedAtBlock ||
		options.IncludeData.TransactionIndexInBlock ||
		options.IncludeData.OperationIndexInTransaction {
		fmt.Fprintf(b.queryBuilder,
			" LEFT JOIN numeric_attributes AS sequenceAttrs"+
				" ON e.entity_key = sequenceAttrs.entity_key"+
				" AND e.from_block = sequenceAttrs.from_block"+
				" AND sequenceAttrs.key = '%s'",
			SequenceAttributeKey,
		)
	}

	for i, orderBy := range b.options.OrderByAnnotations {
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

		keyPlaceholder := b.pushArgument(orderBy.Name)

		fmt.Fprintf(b.queryBuilder,
			" LEFT JOIN %[1]s AS %s"+
				" ON %[2]s.entity_key = e.entity_key"+
				" AND %[2]s.from_block = e.from_block"+
				" AND %[2]s.key = %[3]s",

			tableName,
			sortingTable,
			keyPlaceholder,
		)
	}

	err := b.addPaginationArguments()
	if err != nil {
		return nil, fmt.Errorf("error adding the pagination condition: %w", err)
	}

	if b.needsWhere {
		b.queryBuilder.WriteString(" WHERE ")
		b.needsWhere = false
	} else {
		b.queryBuilder.WriteString(" AND ")
	}

	blockArg := b.pushArgument(b.options.AtBlock)
	fmt.Fprintf(b.queryBuilder, "%s BETWEEN e.from_block AND e.to_block", blockArg)

	if b.needsWhere {
		b.queryBuilder.WriteString(" WHERE ")
		b.needsWhere = false
	} else {
		b.queryBuilder.WriteString(" AND ")
	}

	if t.Expression != nil {
		t.Expression.pushWhereConditions(&b)
	}

	b.queryBuilder.WriteString(" ORDER BY ")

	orderColumns := make([]string, 0, len(b.options.OrderBy))
	for _, o := range b.options.OrderBy {
		suffix := ""
		if o.Descending {
			suffix = " DESC"
		}
		orderColumns = append(orderColumns, o.Column.Name+suffix)
	}
	b.queryBuilder.WriteString(strings.Join(orderColumns, ", "))

	fmt.Fprintf(b.queryBuilder, " LIMIT %d", QueryResultCountLimit)

	return &SelectQuery{
		Query: b.queryBuilder.String(),
		Args:  b.args,
	}, nil
}

func (e *Expression) addJoins(j map[string]AttrJoin) {
	e.Or.addJoins(j)
}

func (e *Expression) pushWhereConditions(b *QueryBuilder) {
	b.queryBuilder.WriteString("(")
	e.Or.pushWhereConditions(b)
	b.queryBuilder.WriteString(")")
}

func (e *OrExpression) addJoins(j map[string]AttrJoin) {
	e.Left.addJoins(j)
	for _, r := range e.Right {
		r.addJoins(j)
	}
}

func (e *OrExpression) pushWhereConditions(b *QueryBuilder) {
	e.Left.pushWhereConditions(b)
	for _, r := range e.Right {
		b.queryBuilder.WriteString(" OR ")
		r.pushWhereConditions(b)
	}
}

func (e *OrRHS) addJoins(j map[string]AttrJoin) {
	e.Expr.addJoins(j)
}

func (e *OrRHS) pushWhereConditions(b *QueryBuilder) {
	e.Expr.pushWhereConditions(b)
}

func (e *AndExpression) addJoins(j map[string]AttrJoin) {
	e.Left.addJoins(j)
	for _, r := range e.Right {
		r.addJoins(j)
	}
}

func (e *AndExpression) pushWhereConditions(b *QueryBuilder) {
	e.Left.pushWhereConditions(b)
	for _, r := range e.Right {
		b.queryBuilder.WriteString(" AND ")
		r.pushWhereConditions(b)
	}
}

func (e *AndRHS) addJoins(j map[string]AttrJoin) {
	e.Expr.addJoins(j)
}

func (e *AndRHS) pushWhereConditions(b *QueryBuilder) {
	e.Expr.pushWhereConditions(b)
}

func (e *EqualExpr) addJoins(j map[string]AttrJoin) {
	if e.Paren != nil {
		e.Paren.addJoins(j)
		return
	}

	if e.LessThan != nil {
		e.LessThan.addJoins(j)
		return
	}

	if e.LessOrEqualThan != nil {
		e.LessOrEqualThan.addJoins(j)
		return
	}

	if e.GreaterThan != nil {
		e.GreaterThan.addJoins(j)
		return
	}

	if e.GreaterOrEqualThan != nil {
		e.GreaterOrEqualThan.addJoins(j)
		return
	}

	if e.Glob != nil {
		e.Glob.addJoins(j)
		return
	}

	if e.Assign != nil {
		e.Assign.addJoins(j)
		return
	}

	if e.Inclusion != nil {
		e.Inclusion.addJoins(j)
		return
	}

	panic("This should not happen!")
}

func (e *EqualExpr) pushWhereConditions(b *QueryBuilder) {
	if e.Paren != nil {
		e.Paren.pushWhereConditions(b)
		return
	}

	if e.LessThan != nil {
		e.LessThan.pushWhereConditions(b)
		return
	}

	if e.LessOrEqualThan != nil {
		e.LessOrEqualThan.pushWhereConditions(b)
		return
	}

	if e.GreaterThan != nil {
		e.GreaterThan.pushWhereConditions(b)
		return
	}

	if e.GreaterOrEqualThan != nil {
		e.GreaterOrEqualThan.pushWhereConditions(b)
		return
	}

	if e.Glob != nil {
		e.Glob.pushWhereConditions(b)
		return
	}

	if e.Assign != nil {
		e.Assign.pushWhereConditions(b)
		return
	}

	if e.Inclusion != nil {
		e.Inclusion.pushWhereConditions(b)
		return
	}

	panic("This should not happen!")
}

func (e *Paren) addJoins(j map[string]AttrJoin) {
	e.Nested.addJoins(j)
}

func (e *Paren) pushWhereConditions(b *QueryBuilder) {
	// We already surround every expr with parenthesis, so no need to add more
	e.Nested.pushWhereConditions(b)
}

func (e *LessThan) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Value.Number != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *LessThan) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Value.String != nil {
		argName = b.pushArgument(*e.Value.String)
	} else {
		argName = b.pushArgument(*e.Value.Number)
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value < %s",
		attributeTableAlias(e.Var),
		argName,
	)
}

func (e *LessOrEqualThan) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Value.Number != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *LessOrEqualThan) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Value.String != nil {
		argName = b.pushArgument(*e.Value.String)
	} else {
		argName = b.pushArgument(*e.Value.Number)
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value <= %s",
		attributeTableAlias(e.Var),
		argName,
	)
}

func (e *GreaterThan) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Value.Number != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *GreaterThan) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Value.String != nil {
		argName = b.pushArgument(*e.Value.String)
	} else {
		argName = b.pushArgument(*e.Value.Number)
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value > %s",
		attributeTableAlias(e.Var),
		argName,
	)
}

func (e *GreaterOrEqualThan) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Value.Number != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *GreaterOrEqualThan) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Value.String != nil {
		argName = b.pushArgument(*e.Value.String)
	} else {
		argName = b.pushArgument(*e.Value.Number)
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value >= %s",
		attributeTableAlias(e.Var),
		argName,
	)
}

func (e *Glob) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *Glob) pushWhereConditions(b *QueryBuilder) {
	argName := b.pushArgument(e.Value)

	op := "~"
	if e.IsNot {
		op = "!~"
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value %s %s",
		attributeTableAlias(e.Var),
		op,
		argName,
	)
}

func (e *Equality) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Value.Number != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *Equality) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Value.String != nil {
		argName = b.pushArgument(*e.Value.String)
	} else {
		argName = b.pushArgument(*e.Value.Number)
	}

	op := "="
	if e.IsNot {
		op = "!="
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value %s %s",
		attributeTableAlias(e.Var),
		op,
		argName,
	)
}

func (e *Inclusion) addJoins(j map[string]AttrJoin) {
	tableName := "string_attributes"
	if e.Values.Numbers != nil {
		tableName = "numeric_attributes"
	}

	j[e.Var] = AttrJoin{
		Table: tableName,
		Alias: attributeTableAlias(e.Var),
	}
}

func (e *Inclusion) pushWhereConditions(b *QueryBuilder) {
	argName := ""
	if e.Values.Strings != nil {
		argName = b.pushArgument(e.Values.Strings)
	} else {
		argName = b.pushArgument(e.Values.Numbers)
	}

	op := "IN"
	if e.IsNot {
		op = "NOT IN"
	}

	fmt.Fprintf(
		b.queryBuilder,
		"%s.value %s %s",
		attributeTableAlias(e.Var),
		op,
		argName,
	)
}
