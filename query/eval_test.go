package query

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

var queryOptions = &QueryOptions{}
var log *slog.Logger = slog.Default()

func TestEqualExpr(t *testing.T) {
	expr, err := Parse("name = \"test\"", log)
	require.NoError(t, err)

	res, err := expr.Evaluate(queryOptions)
	require.NoError(t, err)

	block := uint64(0)

	require.ElementsMatch(t,
		[]any{
			"name",
			"test",
			block, block,
		},
		res.Args,
	)

	// Query for a key with special characters
	expr, err = Parse("déçevant = \"non\"", log)
	require.NoError(t, err)

	res, err = expr.Evaluate(queryOptions)
	require.NoError(t, err)

	require.ElementsMatch(t,
		[]any{
			"déçevant",
			"non",
			block, block,
		},
		res.Args,
	)

	expr, err = Parse("بروح = \"ايوة\"", log)
	require.NoError(t, err)

	res, err = expr.Evaluate(queryOptions)
	require.NoError(t, err)

	require.ElementsMatch(t,
		[]any{
			"بروح",
			"ايوة",
			block, block,
		},
		res.Args,
	)

	// But symbols should fail
	_, err = Parse("foo@ = \"bar\"", log)
	require.Error(t, err)
}

func TestNumericEqualExpr(t *testing.T) {
	expr, err := Parse("age = 123", log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestAndExpr(t *testing.T) {
	expr, err := Parse(`age = 123 && name = "abc"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestOrExpr(t *testing.T) {
	expr, err := Parse(`age = 123 || name = "abc"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestParenthesesExpr(t *testing.T) {
	expr, err := Parse(`(name = 123 || name2 = "abc") && name3 = "def" || (name4 = 456 && name5 = 567)`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestOwner(t *testing.T) {
	owner := common.HexToAddress("0x1")

	expr, err := Parse(fmt.Sprintf(`(age = 123 || name = "abc") && $owner = %s`, owner), log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestGlob(t *testing.T) {
	expr, err := Parse(`age ~ "abc"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestNegation(t *testing.T) {
	expr, err := Parse(
		`!(name < 123 || !(name2 = "abc" && name2 != "bcd")) && !(name3 = "def") || name4 = 456`,
		log,
	)

	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestAndExpr_MultipleTerms(t *testing.T) {
	expr, err := Parse(`a = 1 && b = "x" && c = 2 && d = "y"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestOrExpr_MultipleTerms(t *testing.T) {
	expr, err := Parse(`a = 1 || b = "x" || c = 2 || d = "y"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestMixedAndOr_NoParens(t *testing.T) {
	expr, err := Parse(`a = 1 && b = "x" || c = 2 && d = "y"`, log)
	require.NoError(t, err)

	expr.Evaluate(queryOptions)
}

func TestSorting(t *testing.T) {
	expr, err := Parse(`a = 1`, log)
	require.NoError(t, err)

	_, err = expr.Evaluate(&QueryOptions{
		OrderByAnnotations: []OrderByAnnotation{
			{
				Name: "foo",
				Type: "string",
			},
			{
				Name: "bar",
				Type: "numeric",
			},
		},
	})
	require.NoError(t, err)
}
