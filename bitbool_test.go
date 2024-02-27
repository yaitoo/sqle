package sqle

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitBool(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `users` (`id` id NOT NULL,`status` BIT(1), PRIMARY KEY (`id`))")
	require.NoError(t, err)

	result, err := d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 10, BitBool(true))
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 11, BitBool(false))
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 12, true)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 13, false)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var b1 BitBool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 10).Scan(&b1)
	require.NoError(t, err)

	require.EqualValues(t, true, b1)

	var b2 BitBool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 11).Scan(&b2)
	require.NoError(t, err)

	require.EqualValues(t, false, b2)

	var b3 BitBool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 12).Scan(&b3)
	require.NoError(t, err)

	require.EqualValues(t, true, b3)

	var b4 BitBool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 13).Scan(&b4)
	require.NoError(t, err)

	require.EqualValues(t, false, b4)

}
