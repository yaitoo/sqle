package shardid

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
			tableRotate: NoRotate,
			sequence:    0,
		},
		{
			name:        "build_max_values_should_work",
			timeNow:     time.UnixMilli(TimeEnd),
			workerID:    MaxWorkerID,
			databaseID:  MaxDatabaseID,
			tableRotate: DailyRotate,
			sequence:    MaxSequence,
		},
		{
			name:        "build_should_work",
			timeNow:     time.Now(),
			workerID:    int8(rand.Intn(4)),     // skipcq: GSC-G404
			databaseID:  int16(rand.Intn(1024)), // skipcq: GSC-G404
			tableRotate: WeeklyRotate,
			sequence:    int16(rand.Intn(1024)), // skipcq: GSC-G404
		},
		{
			name:        "id_should_orderable",
			timeNow:     time.Now(),
			workerID:    0,
			databaseID:  0,
			tableRotate: MonthlyRotate,
			sequence:    0,
			orderby:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := Build(test.timeNow.UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence)

			require.Equal(t, test.timeNow.UnixMilli(), id.Time.UnixMilli())
			require.Equal(t, test.workerID, id.WorkerID)
			require.Equal(t, test.databaseID, id.DatabaseID)
			require.Equal(t, test.tableRotate, id.TableRotate)
			require.Equal(t, test.sequence, id.Sequence)

			switch test.tableRotate {
			case NoRotate:
				require.Equal(t, "", id.RotateName())
			case MonthlyRotate:
				require.Equal(t, test.timeNow.UTC().Format("_200601"), id.RotateName())
			case WeeklyRotate:
				_, week := test.timeNow.UTC().ISOWeek()
				require.Equal(t, test.timeNow.UTC().Format("_2006")+fmt.Sprintf("%03d", week), id.RotateName())
			case DailyRotate:
				require.Equal(t, test.timeNow.UTC().Format("_20060102"), id.RotateName())
			default:
				require.Equal(t, "", id.RotateName())
			}

			if test.orderby {
				id2 := Build(test.timeNow.UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence+1)

				id3 := Build(test.timeNow.UnixMilli(), test.workerID+1, test.databaseID, test.tableRotate, test.sequence+2)

				id4 := Build(test.timeNow.Add(1*time.Millisecond).UnixMilli(), test.workerID, test.databaseID, test.tableRotate, test.sequence+3)

				require.Greater(t, id2.Int64, id.Int64)
				require.Greater(t, id3.Int64, id2.Int64)
				require.Greater(t, id4.Int64, id3.Int64)
			}

		})
	}
}

func TestIDInSQL(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `users` (`id` BIGINT NOT NULL,`created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()
	id := Build(now.UnixMilli(), 1, 2, MonthlyRotate, 3)

	result, err := d.Exec("INSERT INTO `users`(`id`, `created`) VALUES(?, ?)", id, now)
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var i ID
	err = d.QueryRow("SELECT `id` FROM `users`").Scan(&i)
	require.NoError(t, err)

	require.Equal(t, id.DatabaseID, i.DatabaseID)
	require.Equal(t, id.Int64, i.Int64)
	require.Equal(t, id.Sequence, i.Sequence)
	require.Equal(t, id.TableRotate, i.TableRotate)
	require.Equal(t, id.Time, i.Time)
	require.Equal(t, id.TimeMillis, i.TimeMillis)
	require.Equal(t, id.WorkerID, i.WorkerID)

}

func TestIdInJSON(t *testing.T) {

	var v int64 = 89166347069554688
	var s = "89166347069554688"

	bufStr, err := json.Marshal(s)
	require.NoError(t, err)

	id := Parse(v)

	bufID, err := json.Marshal(id)
	require.NoError(t, err)

	require.Equal(t, bufStr, bufID)

	var jsID ID
	// Unmarshal id from string json bytes
	err = json.Unmarshal(bufStr, &jsID)
	require.NoError(t, err)
	require.Equal(t, id, jsID)

	var jsString string
	// Unmarshal string from ID json bytes
	err = json.Unmarshal(bufID, &jsString)
	require.NoError(t, err)
	require.Equal(t, s, jsString)
}
