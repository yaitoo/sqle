package sqle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUseOracleParameterize(t *testing.T) {
	tests := []struct {
		name   string
		build  func() *Builder
		assert func(t *testing.T, sql string, params []any)
	}{
		{
			name: "valid_name_emits_named_bind",
			build: func() *Builder {
				b := New()
				UseOracle(b)
				return b.Select("orders", "id").Where("id={user_id}").Param("user_id", 1)
			},
			assert: func(t *testing.T, sql string, params []any) {
				require.Equal(t, "SELECT `id` FROM `orders` WHERE id=:user_id", sql)
				require.Equal(t, []any{1}, params)
			},
		},
		{
			name: "hyphenated_name_falls_back_to_positional",
			build: func() *Builder {
				b := New()
				UseOracle(b)
				return b.Select("orders", "id").Where("id={user-id}").Param("user-id", 1)
			},
			assert: func(t *testing.T, sql string, params []any) {
				require.Equal(t, "SELECT `id` FROM `orders` WHERE id=:p1", sql)
				require.Equal(t, []any{1}, params)
			},
		},
		{
			name: "dotted_name_falls_back_to_positional",
			build: func() *Builder {
				b := New()
				UseOracle(b)
				return b.Select("orders", "id").Where("id={module.field}").Param("module.field", 1)
			},
			assert: func(t *testing.T, sql string, params []any) {
				require.Equal(t, "SELECT `id` FROM `orders` WHERE id=:p1", sql)
				require.Equal(t, []any{1}, params)
			},
		},
		{
			name: "leading_underscore_name_falls_back_to_positional",
			build: func() *Builder {
				b := New()
				UseOracle(b)
				return b.Select("orders", "id").Where("id={_year}").Param("_year", 2024)
			},
			assert: func(t *testing.T, sql string, params []any) {
				require.Equal(t, "SELECT `id` FROM `orders` WHERE id=:p1", sql)
				require.Equal(t, []any{2024}, params)
			},
		},
		{
			// Mix: index increments across both named binds and positional
			// fallbacks so the placeholder count stays monotonic.
			name: "mixed_named_and_positional",
			build: func() *Builder {
				b := New()
				UseOracle(b)
				return b.Select("orders", "id").
					Where("tenant={tenant_id} AND id={user-id} AND year={_year}").
					Param("tenant_id", "acme").
					Param("user-id", 7).
					Param("_year", 2024)
			},
			assert: func(t *testing.T, sql string, params []any) {
				require.Equal(t,
					"SELECT `id` FROM `orders` WHERE tenant=:tenant_id AND id=:p2 AND year=:p3",
					sql)
				require.Equal(t, []any{"acme", 7, 2024}, params)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := test.build()
			sql, params, err := b.Build()
			require.NoError(t, err)
			test.assert(t, sql, params)
		})
	}
}

func TestUsePostgresAndMySQLUnchanged(t *testing.T) {
	// Sanity check: the new Oracle fallback must not affect the other
	// dialects, which already use position-based placeholders and ignore
	// the parameter name entirely.
	t.Run("postgres", func(t *testing.T) {
		b := New()
		UsePostgres(b)
		b.Select("orders", "id").Where("id={user-id}").Param("user-id", 1)
		sql, params, err := b.Build()
		require.NoError(t, err)
		require.Equal(t, "SELECT `id` FROM `orders` WHERE id=$1", sql)
		require.Equal(t, []any{1}, params)
	})

	t.Run("mysql", func(t *testing.T) {
		b := New()
		UseMySQL(b)
		b.Select("orders", "id").Where("id={user-id}").Param("user-id", 1)
		sql, params, err := b.Build()
		require.NoError(t, err)
		require.Equal(t, "SELECT `id` FROM `orders` WHERE id=?", sql)
		require.Equal(t, []any{1}, params)
	})
}