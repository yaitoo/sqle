package sqle

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBool(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `users` (`id` int NOT NULL,`status` BIT(1),`status_string` VARCHAR(5),`status_int` INT, `status_boolean` boolean, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	result, err := d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 10, Bool(true))
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `status`) VALUES(?, ?)", 11, Bool(false))
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

	var b1 Bool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 10).Scan(&b1)
	require.NoError(t, err)

	require.EqualValues(t, true, b1)

	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=? AND status=1", 10).Scan(&b1)
	require.NoError(t, err)

	require.EqualValues(t, true, b1)

	var b2 Bool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 11).Scan(&b2)
	require.NoError(t, err)

	require.EqualValues(t, false, b2)

	var b3 Bool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 12).Scan(&b3)
	require.NoError(t, err)

	require.EqualValues(t, true, b3)

	var b4 Bool
	err = d.QueryRow("SELECT `status` FROM `users` WHERE id=?", 13).Scan(&b4)
	require.NoError(t, err)

	require.EqualValues(t, false, b4)

	// Insert with different types
	_, err = d.Exec("INSERT INTO `users`(`id`, `status_string`) VALUES(?, ?)", 14, "true")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`, `status_string`) VALUES(?, ?)", 15, "false")
	require.NoError(t, err)

	var b5 Bool
	err = d.QueryRow("SELECT `status_string` FROM `users` WHERE id=?", 14).Scan(&b5)
	require.NoError(t, err)

	require.EqualValues(t, true, b5)

	var b6 Bool
	err = d.QueryRow("SELECT `status_string` FROM `users` WHERE id=?", 15).Scan(&b6)
	require.NoError(t, err)

	require.EqualValues(t, false, b6)

	_, err = d.Exec("INSERT INTO `users`(`id`, `status_int`) VALUES(?, ?)", 16, 1)
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`, `status_int`) VALUES(?, ?)", 17, 0)
	require.NoError(t, err)

	var b7 Bool
	err = d.QueryRow("SELECT `status_int` FROM `users` WHERE id=?", 16).Scan(&b7)
	require.NoError(t, err)

	require.EqualValues(t, true, b7)

	var b8 Bool
	err = d.QueryRow("SELECT `status_int` FROM `users` WHERE id=?", 17).Scan(&b8)
	require.NoError(t, err)

	require.EqualValues(t, false, b8)

	_, err = d.Exec("INSERT INTO `users`(`id`, `status_boolean`) VALUES(?, ?)", 18, true)
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`, `status_boolean`) VALUES(?, ?)", 19, false)
	require.NoError(t, err)

	var b9 Bool
	err = d.QueryRow("SELECT `status_boolean` FROM `users` WHERE id=?", 18).Scan(&b9)
	require.NoError(t, err)

	require.EqualValues(t, true, b9)

	var b10 Bool
	err = d.QueryRow("SELECT `status_boolean` FROM `users` WHERE id=?", 19).Scan(&b10)
	require.NoError(t, err)

	require.EqualValues(t, false, b10)

}
