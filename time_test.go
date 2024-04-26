package sqle

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeInSQL(t *testing.T) {

	now := time.Now()
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `times` (`id` id NOT NULL,`created_at` datetime, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	result, err := d.Exec("INSERT INTO `times`(`id`) VALUES(?)", 10)
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `times`(`id`, `created_at`) VALUES(?, ?)", 20, now)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var t10 Time
	err = d.QueryRow("SELECT `created_at` FROM `times` WHERE id=?", 10).Scan(&t10)
	require.NoError(t, err)

	require.EqualValues(t, false, t10.Valid)

	var t20 Time
	err = d.QueryRow("SELECT `created_at` FROM `times` WHERE id=?", 20).Scan(&t20)
	require.NoError(t, err)

	require.EqualValues(t, true, t20.Valid)
	require.EqualValues(t, now.UTC(), t20.Time().UTC())

	result, err = d.Exec("INSERT INTO `times`(`id`,`created_at`) VALUES(?, ?)", 11, t10)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `times`(`id`, `created_at`) VALUES(?, ?)", 21, t20)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var t11 Time
	err = d.QueryRow("SELECT `created_at` FROM `times` WHERE id=?", 11).Scan(&t11)
	require.NoError(t, err)

	require.EqualValues(t, false, t11.Valid)

	var t21 Time
	err = d.QueryRow("SELECT `created_at` FROM `times` WHERE id=?", 21).Scan(&t21)
	require.NoError(t, err)

	require.EqualValues(t, true, t21.Valid)
	require.EqualValues(t, now.UTC(), t21.Time().UTC())

}

func TestTimeInJSON(t *testing.T) {

	sysTime := time.Now()

	bufSysTime, err := json.Marshal(sysTime)
	require.NoError(t, err)

	sqleTime := NewTime(sysTime, true)

	bufSqleTime, err := json.Marshal(sqleTime)
	require.NoError(t, err)

	require.Equal(t, bufSysTime, bufSqleTime)

	var jsSqleTime Time
	// Unmarshal sqle.Time from time.Time json bytes
	err = json.Unmarshal(bufSysTime, &jsSqleTime)
	require.NoError(t, err)

	require.True(t, sysTime.Equal(jsSqleTime.Time()))
	require.Equal(t, true, jsSqleTime.Valid)

	var jsSysTime time.Time
	// Unmarshal time.Time from sqle.Time json bytes
	err = json.Unmarshal(bufSqleTime, &jsSysTime)
	require.NoError(t, err)
	require.True(t, sysTime.Equal(jsSysTime))

	var nullTime Time
	err = json.Unmarshal(nil, &nullTime)
	require.NoError(t, err)
	require.Equal(t, false, nullTime.Valid)
}
