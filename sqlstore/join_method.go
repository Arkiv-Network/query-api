package sqlstore

import (
	"fmt"
	"strings"

	"github.com/Arkiv-Network/sqlite-store/query"
)

type JoinEvaluator struct{}

var _ query.QueryEvaluator = JoinEvaluator{}

type AttrJoin struct {
	Table string
	Alias string
}

func (e JoinEvaluator) EvaluateAST(a *query.AST, options *query.QueryOptions) (*query.SelectQuery, error) {
	b := QueryBuilder{
		options:      *options,
		queryBuilder: &strings.Builder{},
		args:         []any{},
		needsWhere:   true,
	}

	b.queryBuilder.WriteString(strings.Join(
		[]string{
			"SELECT",
			b.options.ColumnString(),
			"FROM payloads AS e",
		},
		" ",
	))

	if a.Expr != nil {
		attrJoins := make(map[string]AttrJoin)
		e.addJoinsOr(&a.Expr.Or, attrJoins)

		for attrName, join := range attrJoins {
			attrPlaceholder := b.PushArgument(attrName)
			fmt.Fprintf(
				b.queryBuilder,
				" INNER JOIN %[1]s AS %[2]s ON e.entity_key = %[2]s.entity_key AND e.from_block = %[2]s.from_block AND %[2]s.key = %[3]s",
				join.Table,
				join.Alias,
				attrPlaceholder,
			)
		}
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

		keyPlaceholder := b.PushArgument(orderBy.Name)

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

	err := query.AddPaginationArguments(&b)
	if err != nil {
		return nil, fmt.Errorf("error adding the pagination condition: %w", err)
	}

	if b.needsWhere {
		b.queryBuilder.WriteString(" WHERE ")
		b.needsWhere = false
	} else {
		b.queryBuilder.WriteString(" AND ")
	}

	blockArg := b.PushArgument(b.options.AtBlock)
	fmt.Fprintf(b.queryBuilder, "%s BETWEEN e.from_block AND e.to_block - 1", blockArg)

	if a.Expr != nil {
		if b.needsWhere {
			b.queryBuilder.WriteString(" WHERE ")
			b.needsWhere = false
		} else {
			b.queryBuilder.WriteString(" AND ")
		}

		e.pushWhereConditionsExpr(a.Expr, &b)
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

	fmt.Fprintf(b.queryBuilder, " LIMIT %d", query.QueryResultCountLimit)

	return &query.SelectQuery{
		Query: b.queryBuilder.String(),
		Args:  b.args,
	}, nil
}

func (e JoinEvaluator) pushWhereConditionsExpr(expr *query.ASTExpr, b *QueryBuilder) {
	b.queryBuilder.WriteString("(")
	e.pushWhereConditionsOr(&expr.Or, b)
	b.queryBuilder.WriteString(")")
}

func (e JoinEvaluator) addJoinsOr(expr *query.ASTOr, j map[string]AttrJoin) {
	for _, r := range expr.Terms {
		e.addJoinsAnd(&r, j)
	}
}

func (e JoinEvaluator) pushWhereConditionsOr(expr *query.ASTOr, b *QueryBuilder) {
	e.pushWhereConditionsAnd(&expr.Terms[0], b)
	for _, r := range expr.Terms[1:] {
		b.queryBuilder.WriteString(" OR ")
		e.pushWhereConditionsAnd(&r, b)
	}
}

func (e JoinEvaluator) addJoinsAnd(expr *query.ASTAnd, j map[string]AttrJoin) {
	e.addJoinsTerm(&expr.Terms[0], j)
	for _, r := range expr.Terms[1:] {
		e.addJoinsTerm(&r, j)
	}
}

func (e JoinEvaluator) pushWhereConditionsAnd(expr *query.ASTAnd, b *QueryBuilder) {
	e.pushWhereConditionsTerm(&expr.Terms[0], b)
	for _, r := range expr.Terms[1:] {
		b.queryBuilder.WriteString(" AND ")
		e.pushWhereConditionsTerm(&r, b)
	}
}

func (JoinEvaluator) addJoinsTerm(e *query.ASTTerm, j map[string]AttrJoin) {
	if e.LessThan != nil {
		tableName := "string_attributes"
		if e.LessThan.Value.Number != nil {
			tableName = "numeric_attributes"
		}

		j[e.LessThan.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.LessThan.Var),
		}
		return
	}

	if e.LessOrEqualThan != nil {
		tableName := "string_attributes"
		if e.LessOrEqualThan.Value.Number != nil {
			tableName = "numeric_attributes"
		}

		j[e.LessOrEqualThan.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.LessOrEqualThan.Var),
		}
		return
	}

	if e.GreaterThan != nil {
		tableName := "string_attributes"
		if e.GreaterThan.Value.Number != nil {
			tableName = "numeric_attributes"
		}

		j[e.GreaterThan.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.GreaterThan.Var),
		}
		return
	}

	if e.GreaterOrEqualThan != nil {
		tableName := "string_attributes"
		if e.GreaterOrEqualThan.Value.Number != nil {
			tableName = "numeric_attributes"
		}

		j[e.GreaterOrEqualThan.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.GreaterOrEqualThan.Var),
		}
		return
	}

	if e.Glob != nil {
		tableName := "string_attributes"

		j[e.Glob.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.Glob.Var),
		}
		return
	}

	if e.Assign != nil {
		tableName := "string_attributes"
		if e.Assign.Value.Number != nil {
			tableName = "numeric_attributes"
		}

		j[e.Assign.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.Assign.Var),
		}
		return
	}

	if e.Inclusion != nil {
		tableName := "string_attributes"
		if e.Inclusion.Values.Numbers != nil {
			tableName = "numeric_attributes"
		}

		j[e.Inclusion.Var] = AttrJoin{
			Table: tableName,
			Alias: attributeTableAlias(e.Inclusion.Var),
		}
		return
	}

	panic("This should not happen!")
}

func (JoinEvaluator) pushWhereConditionsTerm(e *query.ASTTerm, b *QueryBuilder) {
	if e.LessThan != nil {
		argName := ""
		if e.LessThan.Value.String != nil {
			argName = b.PushArgument(*e.LessThan.Value.String)
		} else {
			argName = b.PushArgument(*e.LessThan.Value.Number)
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value < %s",
			attributeTableAlias(e.LessThan.Var),
			argName,
		)
		return
	}

	if e.LessOrEqualThan != nil {
		argName := ""
		if e.LessOrEqualThan.Value.String != nil {
			argName = b.PushArgument(*e.LessOrEqualThan.Value.String)
		} else {
			argName = b.PushArgument(*e.LessOrEqualThan.Value.Number)
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value <= %s",
			attributeTableAlias(e.LessOrEqualThan.Var),
			argName,
		)
		return
	}

	if e.GreaterThan != nil {
		argName := ""
		if e.GreaterThan.Value.String != nil {
			argName = b.PushArgument(*e.GreaterThan.Value.String)
		} else {
			argName = b.PushArgument(*e.GreaterThan.Value.Number)
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value > %s",
			attributeTableAlias(e.GreaterThan.Var),
			argName,
		)
		return
	}

	if e.GreaterOrEqualThan != nil {
		argName := ""
		if e.GreaterOrEqualThan.Value.String != nil {
			argName = b.PushArgument(*e.GreaterOrEqualThan.Value.String)
		} else {
			argName = b.PushArgument(*e.GreaterOrEqualThan.Value.Number)
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value >= %s",
			attributeTableAlias(e.GreaterOrEqualThan.Var),
			argName,
		)
		return
	}

	if e.Glob != nil {
		argName := b.PushArgument(e.Glob.Value)

		op := "~"
		if e.Glob.IsNot {
			op = "!~"
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value %s %s",
			attributeTableAlias(e.Glob.Var),
			op,
			argName,
		)
		return
	}

	if e.Assign != nil {
		argName := ""
		if e.Assign.Value.String != nil {
			argName = b.PushArgument(*e.Assign.Value.String)
		} else {
			argName = b.PushArgument(*e.Assign.Value.Number)
		}

		op := "="
		if e.Assign.IsNot {
			op = "!="
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value %s %s",
			attributeTableAlias(e.Assign.Var),
			op,
			argName,
		)
		return
	}

	if e.Inclusion != nil {
		args := []any{}
		if e.Inclusion.Values.Strings != nil {
			for _, s := range e.Inclusion.Values.Strings {
				args = append(args, s)
			}
		} else {
			for _, s := range e.Inclusion.Values.Numbers {
				args = append(args, s)
			}
		}

		argNames := []string{}
		for _, arg := range args {
			argNames = append(argNames, b.PushArgument(arg))
		}

		op := "IN"
		if e.Inclusion.IsNot {
			op = "NOT IN"
		}

		fmt.Fprintf(
			b.queryBuilder,
			"%s.value %s (%s)",
			attributeTableAlias(e.Inclusion.Var),
			op,
			strings.Join(argNames, ", "),
		)
		return
	}

	panic("This should not happen!")
}
