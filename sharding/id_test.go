package sharding

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestID(t *testing.T) {
	tests := []struct {
		name        string
		build       func() int64
		timeNow     time.Time
		workerID    int8
		databaseID  int16
		tableRotate TableRotate
		sequence    int16
		orderby     bool
	}{
		{
			name:        "build_min_values_should_work",
			timeNow:     time.UnixMilli(TimeEpoch),
			workerID:    0,
			databaseID:  0,
			tableRotate: None,
			sequence:    0,
		},
		{
			name:        "build_max_values_should_work",
			timeNow:     time.UnixMilli(TimeEnd),
			workerID:    MaxWorkerID,
			databaseID:  MaxDatabaseID,
			tableRotate: Daily,
			sequence:    MaxSequence,
		},
		{
			name:        "build_should_work",
			timeNow:     time.Now(),
			workerID:    int8(rand.Intn(4)),
			databaseID:  int16(rand.Intn(1024)),
			tableRotate: Weekly,
			sequence:    int16(rand.Intn(1024)),
		},
		{
			name:        "id_should_orderable",
			timeNow:     time.Now(),
			workerID:    0,
			databaseID:  0,
			tableRotate: Monthly,
			sequence:    0,
			orderby:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := Build(test.timeNow.UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence)

			result := Parse(id)

			require.Equal(t, test.timeNow.UnixMilli(), result.Time.UnixMilli())
			require.Equal(t, test.workerID, result.WorkerID)
			require.Equal(t, test.databaseID, result.DatabaseID)
			require.Equal(t, test.tableRotate, result.TableRotate)
			require.Equal(t, test.sequence, result.Sequence)

			switch test.tableRotate {
			case None:
				require.Equal(t, "", result.RotateName())
			case Monthly:
				require.Equal(t, test.timeNow.UTC().Format("200601"), result.RotateName())
			case Weekly:
				_, week := test.timeNow.UTC().ISOWeek()
				require.Equal(t, test.timeNow.UTC().Format("2006")+fmt.Sprintf("%03d", week), result.RotateName())
			case Daily:
				require.Equal(t, test.timeNow.UTC().Format("20060102"), result.RotateName())
			default:
				require.Equal(t, "", result.RotateName())
			}

			if test.orderby {
				id2 := Build(test.timeNow.UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence+1)

				id3 := Build(test.timeNow.UnixMilli(), test.workerID+1, test.databaseID, test.tableRotate, test.sequence+2)

				id4 := Build(test.timeNow.Add(1*time.Millisecond).UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence+3)

				require.Greater(t, id2, id)
				require.Greater(t, id3, id2)
				require.Greater(t, id4, id3)
			}

		})
	}
}
