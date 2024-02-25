package sharding

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerator(t *testing.T) {
	tests := []struct {
		name   string
		new    func() *Generator
		assert func(t *testing.T, gen *Generator)
	}{
		{
			name: "sequence_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, None, 0)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, None, 1)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, None, 2)
				require.Equal(t, want, id)

			},
		},
		{
			name: "worker_id_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, None, 0)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, None, 1)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, None, 2)
				require.Equal(t, want, id)

			},
		},
		{
			name: "database_id_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, None, 0)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, None, 1)
				require.Equal(t, want, id)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 2, None, 2)
				require.Equal(t, want, id)

			},
		},
		{
			name: "monthly_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithTableRotate(Monthly))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, Monthly, 0)
				require.Equal(t, want, id)

				md := From(id)
				require.Equal(t, "202402", md.RotateName())
			},
		},
		{
			name: "weekly_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithTableRotate(Weekly))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, Weekly, 0)
				require.Equal(t, want, id)

				md := From(id)
				require.Equal(t, "2024008", md.RotateName())
			},
		},
		{
			name: "daily_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithTableRotate(Daily))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, Daily, 0)
				require.Equal(t, want, id)

				md := From(id)
				require.Equal(t, "20240220", md.RotateName())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := test.new()
			test.assert(t, g)
		})
	}
}
