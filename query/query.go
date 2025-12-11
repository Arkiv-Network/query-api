package query

import (
	"fmt"
	"hash/fnv"
	"strings"
)

type SelectQuery struct {
	Query string
	Args  []any
}

type QueryBuilder struct {
	queryBuilder *strings.Builder
	args         []any
	argsCount    uint32
	tableCounter uint32
	needsComma   bool
	needsWhere   bool
	options      QueryOptions
	sqlDialect   string
}

func attributeTableAlias(name string) string {
	h := fnv.New32a()
	h.Write([]byte(name))

	return fmt.Sprintf("arkiv_attr_%d", h.Sum32())
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
		b.queryBuilder.WriteString(", ")
	} else {
		b.needsComma = true
	}
}

func (b *QueryBuilder) addPaginationArguments() error {
	paginationConditions := []string{}

	if len(b.options.Cursor) > 0 {
		// Pre-allocate argument counters so that we don't need to duplicate them below
		args := make([]string, 0, len(b.options.Cursor))
		for _, val := range b.options.Cursor {
			args = append(args, b.pushArgument(val.Value))
		}

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

				arg := args[j]

				columnIx, err := b.options.GetColumnIndex(from.ColumnName)
				if err != nil {
					return fmt.Errorf("error getting column index: %w", err)
				}
				column := b.options.Columns[columnIx]

				subcondition = append(
					subcondition,
					fmt.Sprintf("%s %s %s", column.QualifiedName, operator, arg),
				)
			}

			paginationConditions = append(
				paginationConditions,
				fmt.Sprintf("(%s)", strings.Join(subcondition, " AND ")),
			)
		}

		paginationCondition := strings.Join(paginationConditions, " OR ")

		if b.needsWhere {
			b.queryBuilder.WriteString(" WHERE ")
			b.needsWhere = false
		} else {
			b.queryBuilder.WriteString(" AND ")
		}

		b.queryBuilder.WriteString("(")
		b.queryBuilder.WriteString(paginationCondition)
		b.queryBuilder.WriteString(")")
	}

	return nil
}
