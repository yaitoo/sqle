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
