package shardid

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
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, NoRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, NoRotate, 1)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, NoRotate, 2)
				require.Equal(t, want.Int64, id.Int64)

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
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 1)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 2)
				require.Equal(t, want.Int64, id.Int64)

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
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, NoRotate, 1)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 2, NoRotate, 2)
				require.Equal(t, want.Int64, id.Int64)

			},
		},
		{
			name: "database_id_should_reset",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(2))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, NoRotate, 1)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, NoRotate, 2)
				require.Equal(t, want.Int64, id.Int64)

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 1, NoRotate, 3)
				require.Equal(t, want.Int64, id.Int64)

			},
		},
		{
			name: "monthly_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithMonthlyRotate())

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, MonthlyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)
				require.Equal(t, "202402", id.RotateName())
			},
		},
		{
			name: "weekly_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithWeeklyRotate())

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, WeeklyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "2024008", id.RotateName())
			},
		},
		{
			name: "daily_rotate_should_work",
			new: func() *Generator {
				g := New(WithTimeNow(func() time.Time {
					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)
				}), WithWorkerID(1), WithDatabase(3), WithDailyRotate())

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, DailyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "20240220", id.RotateName())
			},
		},
		{
			name: "sequence_overflows_capacity_should_work",
			new: func() *Generator {
				i := 0
				g := New(WithTimeNow(func() time.Time {
					defer func() {
						i++
					}()

					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Millisecond)

				}), WithWorkerID(1), WithRotate(DailyRotate))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				gen.nextSequence = MaxSequence
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, DailyRotate, MaxSequence)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "20240220", id.RotateName())

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(1*time.Millisecond).UnixMilli(), 1, 0, DailyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "20240220", id.RotateName())
			},
		},
		{
			name: "time_move_backwards_should_work",
			new: func() *Generator {
				i := 0
				g := New(WithTimeNow(func() time.Time {
					defer func() {
						i++
					}()

					if i == 1 {
						return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(-1 * time.Millisecond)
					}

					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Millisecond)

				}), WithWorkerID(1), WithRotate(DailyRotate))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, DailyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)
				require.Equal(t, "20240220", id.RotateName())

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(1*time.Millisecond).UnixMilli(), 1, 0, DailyRotate, 1)
				require.Equal(t, want.Int64, id.Int64)
				require.Equal(t, "20240220", id.RotateName())

			},
		},
		{
			name: "time_move_backwards_and_sequence_overflows_capacity_should_work",
			new: func() *Generator {
				i := 0
				g := New(WithTimeNow(func() time.Time {
					defer func() {
						i++
					}()

					if i == 1 {
						return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(-1 * time.Millisecond)
					}

					return time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Millisecond)

				}), WithWorkerID(1), WithRotate(DailyRotate))

				return g
			},
			assert: func(t *testing.T, gen *Generator) {
				gen.nextSequence = MaxSequence
				id := gen.Next()
				want := Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 1, 0, DailyRotate, MaxSequence)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "20240220", id.RotateName())

				id = gen.Next()
				want = Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).Add(2*time.Millisecond).UnixMilli(), 1, 0, DailyRotate, 0)
				require.Equal(t, want.Int64, id.Int64)

				require.Equal(t, "20240220", id.RotateName())

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
