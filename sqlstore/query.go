package sqlstore

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/Arkiv-Network/sqlite-store/query"
)

type QueryBuilder struct {
	queryBuilder *strings.Builder
	args         []any
	argsCount    uint32
	tableCounter uint32
	needsWhere   bool
	options      query.QueryOptions
}

var _ query.Builder = &QueryBuilder{}

func attributeTableAlias(name string) string {
	h := fnv.New32a()
	h.Write([]byte(name))

	return fmt.Sprintf("arkiv_attr_%d", h.Sum32())
}

func (b *QueryBuilder) PushArgument(arg any) string {
	b.args = append(b.args, arg)
	b.argsCount += 1
	return fmt.Sprintf("$%d", b.argsCount)
}

func (b *QueryBuilder) GetOptions() *query.QueryOptions {
	return &b.options
}

func (b *QueryBuilder) WriteWhereClause(condition string) {
	if b.needsWhere {
		b.queryBuilder.WriteString(" WHERE ")
		b.needsWhere = false
	} else {
		b.queryBuilder.WriteString(" AND ")
	}

	b.queryBuilder.WriteString("(")
	b.queryBuilder.WriteString(condition)
	b.queryBuilder.WriteString(")")
}
