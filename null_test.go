package sqle

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNullInSQL(t *testing.T) {

	v := float64(10.5)
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `nulls` (`id` int NOT NULL,`value` DECIMAL(10, 2), PRIMARY KEY (`id`))")
	require.NoError(t, err)

	result, err := d.Exec("INSERT INTO `nulls`(`id`) VALUES(?)", 10)
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `nulls`(`id`, `value`) VALUES(?, ?)", 20, v)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var v10 Null[float64]
	err = d.QueryRow("SELECT `value` FROM `nulls` WHERE id=?", 10).Scan(&v10)
	require.NoError(t, err)

	require.EqualValues(t, false, v10.Valid)

	var v20 Null[float64]
	err = d.QueryRow("SELECT `value` FROM `nulls` WHERE id=?", 20).Scan(&v20)
	require.NoError(t, err)

	require.EqualValues(t, true, v20.Valid)
	require.EqualValues(t, v, v20.TValue())

	result, err = d.Exec("INSERT INTO `nulls`(`id`,`value`) VALUES(?, ?)", 11, v10)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `nulls`(`id`, `value`) VALUES(?, ?)", 21, v20)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var v11 Null[float64]
	err = d.QueryRow("SELECT `value` FROM `nulls` WHERE id=?", 11).Scan(&v11)
	require.NoError(t, err)

	require.EqualValues(t, false, v11.Valid)

	var v21 Null[float64]
	err = d.QueryRow("SELECT `value` FROM `nulls` WHERE id=?", 21).Scan(&v21)
	require.NoError(t, err)

	require.EqualValues(t, true, v21.Valid)
	require.EqualValues(t, v, v21.TValue())

}

func TestNullInJSON(t *testing.T) {

	sysValue := 10.5

	bufSysValue, err := json.Marshal(sysValue)
	require.NoError(t, err)

	sqleNull := NewNull(sysValue, true)

	bufSqleNull, err := json.Marshal(sqleNull)
	require.NoError(t, err)

	require.Equal(t, bufSysValue, bufSqleNull)

	var jsSqleValue Null[float64]
	// Unmarshal sqle.Time from time.Time json bytes
	err = json.Unmarshal(bufSysValue, &jsSqleValue)
	require.NoError(t, err)

	require.Equal(t, sysValue, jsSqleValue.TValue())
	require.Equal(t, true, jsSqleValue.Valid)

	var jsSysValue float64
	// Unmarshal time.Time from sqle.Time json bytes
	err = json.Unmarshal(bufSqleNull, &jsSysValue)
	require.NoError(t, err)
	require.Equal(t, sysValue, jsSysValue)

	var nullValue Null[float64]
	err = json.Unmarshal([]byte("null"), &nullValue)
	require.NoError(t, err)
	require.Equal(t, false, nullValue.Valid)

	bufNull, err := json.Marshal(nullValue)
	require.NoError(t, err)
	require.Equal(t, []byte("null"), bufNull)
}
