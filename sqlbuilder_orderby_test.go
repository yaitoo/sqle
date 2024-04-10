package sqle

import (
	"testing"

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
				b.OrderBy().
					Desc("created_at").
					Asc("id", "name").
					Asc("updated_at")

				return b
			},
			wanted: "SELECT * FROM users ORDER BY created_at DESC, id ASC, name ASC, updated_at ASC",
		},
		{
			name: "safe_columns_should_work",
			build: func() *Builder {
				b := New("SELECT * FROM users")
				b.OrderBy("id", "updated_at").
					Asc("id", "name").
					Desc("created_at", "unsafe_input").
					Asc("updated_at")

				return b
			},
			wanted: "SELECT * FROM users ORDER BY id ASC, updated_at ASC",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.build().String()

			require.Equal(t, test.wanted, actual)
		})
	}
}
