package sqle

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringInSQL(t *testing.T) {

	v := "has value"
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `strings` (`id` int NOT NULL,`name` varchar(125), PRIMARY KEY (`id`))")
	require.NoError(t, err)

	result, err := d.Exec("INSERT INTO `strings`(`id`) VALUES(?)", 10)
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `strings`(`id`, `name`) VALUES(?, ?)", 20, v)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var v10 String
	err = d.QueryRow("SELECT `name` FROM `strings` WHERE id=?", 10).Scan(&v10)
	require.NoError(t, err)

	require.EqualValues(t, false, v10.Valid)

	var v20 String
	err = d.QueryRow("SELECT `name` FROM `strings` WHERE id=?", 20).Scan(&v20)
	require.NoError(t, err)

	require.EqualValues(t, true, v20.Valid)
	require.EqualValues(t, v, v20.String())

	result, err = d.Exec("INSERT INTO `strings`(`id`,`name`) VALUES(?, ?)", 11, v10)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `strings`(`id`, `name`) VALUES(?, ?)", 21, v20)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var v11 String
	err = d.QueryRow("SELECT `name` FROM `strings` WHERE id=?", 11).Scan(&v11)
	require.NoError(t, err)

	require.EqualValues(t, false, v11.Valid)

	var v21 String
	err = d.QueryRow("SELECT `name` FROM `strings` WHERE id=?", 21).Scan(&v21)
	require.NoError(t, err)

	require.EqualValues(t, true, v21.Valid)
	require.EqualValues(t, v, v21.String())

}

func TestStringInJSON(t *testing.T) {

	sysString := "has value"

	bufSysString, err := json.Marshal(sysString)
	require.NoError(t, err)

	sqleString := NewString(sysString)

	bufSqleString, err := json.Marshal(sqleString)
	require.NoError(t, err)

	require.Equal(t, bufSysString, bufSqleString)

	var jsSqleString String
	// Unmarshal sqle.Time from time.Time json bytes
	err = json.Unmarshal(bufSysString, &jsSqleString)
	require.NoError(t, err)

	require.Equal(t, sysString, jsSqleString.String())
	require.Equal(t, true, jsSqleString.Valid)

	var jsSysString string
	// Unmarshal time.Time from sqle.Time json bytes
	err = json.Unmarshal(bufSqleString, &jsSysString)
	require.NoError(t, err)
	require.Equal(t, sysString, jsSysString)

	var nullString String
	err = json.Unmarshal([]byte("null"), &nullString)
	require.NoError(t, err)
	require.Equal(t, false, nullString.Valid)

	bufNull, err := json.Marshal(nullString)
	require.NoError(t, err)
	require.Equal(t, []byte("null"), bufNull)
}
