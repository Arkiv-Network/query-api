package query

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func pointerOf[T any](v T) *T {
	return &v
}

func TestParse(t *testing.T) {
	t.Run("quoted string", func(t *testing.T) {
		v, err := Parse(`name = "test\"2"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: false,
									Value: Value{
										String: pointerOf("test\"2"),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("empty query", func(t *testing.T) {
		_, err := Parse(``, log)
		require.Error(t, err)
	})

	t.Run("all", func(t *testing.T) {
		v, err := Parse(`$all`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: nil,
			},
			v,
		)
	})

	t.Run("number", func(t *testing.T) {
		v, err := Parse(`name = 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: false,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("not parentheses", func(t *testing.T) {
		v, err := Parse(`!(name = 123 || name = 456)`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: true,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
							Right: []*AndRHS{
								{
									EqualExpr{
										Assign: &Equality{
											Var:   "name",
											IsNot: true,
											Value: Value{
												Number: pointerOf(uint64(456)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("not number", func(t *testing.T) {
		v, err := Parse(`!(name = 123)`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: true,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("not equal", func(t *testing.T) {
		v, err := Parse(`name != 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: true,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("lessthan", func(t *testing.T) {
		v, err := Parse(`name < 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								LessThan: &LessThan{
									Var: "name",
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)

		v, err = Parse(`name < "123"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								LessThan: &LessThan{
									Var: "name",
									Value: Value{
										String: pointerOf("123"),
									},
								},
							},
						},
					},
				},
			},
			v,
		)

		v, err = Parse(`!(name < 123)`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								GreaterOrEqualThan: &GreaterOrEqualThan{
									Var: "name",
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("lessthanequal", func(t *testing.T) {
		v, err := Parse(`name <= 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								LessOrEqualThan: &LessOrEqualThan{
									Var: "name",
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)

		v, err = Parse(`name <= "123"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								LessOrEqualThan: &LessOrEqualThan{
									Var: "name",
									Value: Value{
										String: pointerOf("123"),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("greaterthan", func(t *testing.T) {
		v, err := Parse(`name > 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								GreaterThan: &GreaterThan{
									Var: "name",
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)

		v, err = Parse(`name > "123"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								GreaterThan: &GreaterThan{
									Var: "name",
									Value: Value{
										String: pointerOf("123"),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("greaterthanequal", func(t *testing.T) {
		v, err := Parse(`name >= 123`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								GreaterOrEqualThan: &GreaterOrEqualThan{
									Var: "name",
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
					},
				},
			},
			v,
		)

		v, err = Parse(`name >= "123"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								GreaterOrEqualThan: &GreaterOrEqualThan{
									Var: "name",
									Value: Value{
										String: pointerOf("123"),
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("glob", func(t *testing.T) {
		v, err := Parse(`name ~ "foo"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Glob: &Glob{
									Var:   "name",
									IsNot: false,
									Value: "foo",
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("owner", func(t *testing.T) {
		owner := common.HexToAddress("0x1").Hex()
		v, err := Parse(fmt.Sprintf(`$owner = %s`, owner), log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "$owner",
									IsNot: false,
									Value: Value{
										String: &owner,
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("owner quoted", func(t *testing.T) {
		owner := common.HexToAddress("0x1").Hex()
		v, err := Parse(fmt.Sprintf(`$owner = "%s"`, owner), log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "$owner",
									IsNot: false,
									Value: Value{
										String: &owner,
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("not owner", func(t *testing.T) {
		owner := common.HexToAddress("0x1").Hex()
		v, err := Parse(fmt.Sprintf(`$owner != %s`, owner), log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "$owner",
									IsNot: true,
									Value: Value{
										String: &owner,
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("glob", func(t *testing.T) {
		v, err := Parse(`name ~ "foo"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Glob: &Glob{
									Var:   "name",
									IsNot: false,
									Value: "foo",
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("not glob", func(t *testing.T) {
		v, err := Parse(`name !~ "foo"`, log)
		require.NoError(t, err)

		require.Equal(
			t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Glob: &Glob{
									Var:   "name",
									IsNot: true,
									Value: "foo",
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("and", func(t *testing.T) {
		v, err := Parse(`(name = 123 && name2 = "abc")`, log)
		require.NoError(t, err)

		require.Equal(t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: false,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
							Right: []*AndRHS{
								{
									Expr: EqualExpr{
										Assign: &Equality{
											Var:   "name2",
											IsNot: false,
											Value: Value{
												String: pointerOf("abc"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("or", func(t *testing.T) {
		v, err := Parse(`name = 123 || name2 = "abc"`, log)
		require.NoError(t, err)

		require.Equal(t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Assign: &Equality{
									Var:   "name",
									IsNot: false,
									Value: Value{
										Number: pointerOf(uint64(123)),
									},
								},
							},
						},
						Right: []*OrRHS{
							{
								Expr: AndExpression{
									Left: EqualExpr{
										Assign: &Equality{
											Var:   "name2",
											IsNot: false,
											Value: Value{
												String: pointerOf("abc"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("parentheses", func(t *testing.T) {
		v, err := Parse(`(name = 123 || name2 = "abc") && (name3 = "def") || name4 = 456`, log)
		require.NoError(t, err)

		require.Equal(t,
			&TopLevel{
				Expression: &Expression{
					Or: OrExpression{
						Left: AndExpression{
							Left: EqualExpr{
								Paren: &Paren{
									Nested: Expression{
										Or: OrExpression{
											Left: AndExpression{
												Left: EqualExpr{
													Assign: &Equality{
														Var:   "name",
														IsNot: false,
														Value: Value{
															Number: pointerOf(uint64(123)),
														},
													},
												},
											},
											Right: []*OrRHS{
												{
													Expr: AndExpression{
														Left: EqualExpr{
															Assign: &Equality{
																Var:   "name2",
																IsNot: false,
																Value: Value{
																	String: pointerOf("abc"),
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
							Right: []*AndRHS{
								{
									Expr: EqualExpr{
										Assign: &Equality{
											Var:   "name3",
											IsNot: false,
											Value: Value{
												String: pointerOf("def"),
											},
										},
									},
								},
							},
						},
						Right: []*OrRHS{
							{
								Expr: AndExpression{
									Left: EqualExpr{
										Assign: &Equality{
											Var:   "name4",
											IsNot: false,
											Value: Value{
												Number: pointerOf(uint64(456)),
											},
										},
									},
								},
							},
						},
					},
				},
			},
			v,
		)
	})

	t.Run("invalid expression", func(t *testing.T) {
		_, err := Parse(`key = 8e`, log)
		require.Error(t, err, `1:8: unexpected token "e"`)
	})

	t.Run("invalid expression", func(t *testing.T) {
		_, err := Parse(`key = 8e`, log)
		require.Error(t, err, `1:8: unexpected token "e"`)
	})

}
