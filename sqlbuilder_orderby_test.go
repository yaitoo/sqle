package sqle

import (
	"testing"

	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/require"
)

func TestOrderByBuilder(t *testing.T) {
	tests := []struct {
		name   string
		build  func() *Builder
		wanted string
	}{
		{
			name: "no_safe_columns_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")
				b.Order().
					ByDesc("created_at").
					ByAsc("id", "name").
					ByAsc("updated_at")

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `created_at` DESC, `id` ASC, `name` ASC, `updated_at` ASC",
		},
		{
			name: "safe_columns_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")
				b.Order(WithAllow("id", "created_at", "updated_at")).
					ByAsc("id", "name").
					ByDesc("created_at", "unsafe_input").
					ByAsc("updated_at")

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `id` ASC, `created_at` DESC, `updated_at` ASC",
		},
		{
			name: "order_by_raw_sql_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")
				b.Order(WithAllow("id", "created_at", "updated_at", "age")).
					By("created_at desc, id, name asc, updated_at asc, age invalid_by,  unsafe_asc, unsafe_desc desc")

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `created_at` DESC, `id` ASC, `updated_at` ASC",
		},
		{
			name: "with_order_by_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")

				ob := NewOrderBy(WithAllow("id", "created_at", "updated_at", "age"))
				ob.By("created_at desc, id, name asc, updated_at asc, age invalid_by,  unsafe_asc, unsafe_desc desc")

				b.WithOrderBy(ob)

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `created_at` DESC, `id` ASC, `updated_at` ASC",
		},
		{
			name: "with_nil_order_by_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")
				b.Order(WithAllow("id", "created_at", "updated_at")).
					ByAsc("id", "name").
					ByDesc("created_at", "unsafe_input").
					ByAsc("updated_at")

				b.WithOrderBy(nil)

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `id` ASC, `created_at` DESC, `updated_at` ASC",
		},
		{
			name: "with_empty_order_by_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")

				ob := NewOrderBy(WithAllow("age")).
					ByAsc("id", "name").
					ByDesc("created_at", "unsafe_input").
					ByAsc("updated_at")

				b.WithOrderBy(ob)

				return b
			},
			wanted: "SELECT * FROM users",
		},
		{
			name: "with_to_name_order_by_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")

				ob := NewOrderBy(WithToName(strcase.ToSnake), WithAllow("created_at")).
					ByAsc("id", "name").
					ByDesc("createdAt", "unsafe_input").
					ByAsc("updated_at")

				b.WithOrderBy(ob)

				return b
			},
			wanted: "SELECT * FROM users ORDER BY `created_at` DESC",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.build().String()

			require.Equal(t, test.wanted, actual)
		})
	}
}

func TestWithOrderBy_ReturnsSameInstanceAndAppends(t *testing.T) {
	b := New("SELECT * FROM users")
	ob := NewOrderBy(WithAllow("id", "name"))
	ob.ByDesc("id")

	// WithOrderBy must return the same *OrderByBuilder that was passed in,
	// not a fresh empty wrapper, so the caller can keep building on it.
	ret := b.WithOrderBy(ob)
	require.Same(t, ob, ret)

	// WithOrderBy appends ob.String() to b.stmt once, at call time. Pin
	// that snapshot so a future change can't quietly retro-append on each
	// chained By* call (which would silently drop columns for callers that
	// never re-invoke WithOrderBy).
	require.Equal(t, "SELECT * FROM users ORDER BY `id` DESC", b.String())

	// Subsequent By* calls on the returned builder mutate ob.Builder, not b.
	ret.ByAsc("name")

	// ob.Builder reflects the chained call...
	require.Equal(t, " ORDER BY `id` DESC, `name` ASC", ob.String())
	// ...but b's snapshot is unchanged — re-call WithOrderBy to push it.
	require.Equal(t, "SELECT * FROM users ORDER BY `id` DESC", b.String())
}

func TestWithOrderBy_RejectsSharedReceiver(t *testing.T) {
	b := New("SELECT * FROM users")

	// ob built via b.Order() shares the receiver — its embedded *Builder
	// is exactly b, so ob.String() reads b.stmt and WithOrderBy would
	// append b's stmt to itself.
	ob := b.Order(WithAllow("id", "name"))
	ob.ByDesc("id")

	// Sanity: b already has the ORDER BY because ob.Builder == b.
	require.Equal(t, "SELECT * FROM users ORDER BY `id` DESC", b.String())

	// WithOrderBy must reject the shared-receiver builder instead of
	// silently doubling b.stmt.
	ret := b.WithOrderBy(ob)

	// b's stmt is unchanged after the rejected call (no doubling).
	require.Equal(t, "SELECT * FROM users ORDER BY `id` DESC", b.String())

	// WithOrderBy returns ob so the caller can still inspect it; the
	// error surfaces from Build.
	require.Same(t, ob, ret)

	_, _, err := b.Build()
	require.ErrorIs(t, err, ErrOrderBySharedReceiver)
}

func TestWithOrderBy_StandaloneBuilderDoesNotMarkError(t *testing.T) {
	b := New("SELECT * FROM users")
	ob := NewOrderBy(WithAllow("id"))
	ob.ByDesc("id")

	b.WithOrderBy(ob)

	// Pin the happy path: a standalone ob must not trip the
	// shared-receiver guard, so Build succeeds.
	_, _, err := b.Build()
	require.NoError(t, err)
}
