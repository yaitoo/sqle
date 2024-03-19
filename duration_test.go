package sqle

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDuration(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `users` (`id` id NOT NULL,`ttl` VARCHAR(20), `b_ttl` VARBINARY, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	d10 := Duration(10 * time.Second)
	d11 := Duration(11 * time.Second)
	d12 := Duration(12 * time.Second)
	d13 := Duration(13 * time.Second)

	result, err := d.Exec("INSERT INTO `users`(`id`, `ttl`,`b_ttl`) VALUES(?, ?, ?)", 10, d10, d10)
	require.NoError(t, err)

	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `ttl`) VALUES(?, ?)", 11, d11)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `ttl`) VALUES(?, ?)", 12, d12)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	result, err = d.Exec("INSERT INTO `users`(`id`, `ttl`) VALUES(?, ?)", 13, d13)
	require.NoError(t, err)

	rows, err = result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)

	var b10 Duration
	var b_b10 Duration
	err = d.QueryRow("SELECT `ttl`, `b_ttl` FROM `users` WHERE id=?", 10).Scan(&b10, &b_b10)
	require.NoError(t, err)

	require.Equal(t, d10.Duration(), b10.Duration())
	require.Equal(t, d10.Duration(), b_b10.Duration())

	var b11 Duration
	var b_b11 Duration
	err = d.QueryRow("SELECT `ttl`, `b_ttl` FROM `users` WHERE id=?", 11).Scan(&b11, &b_b11)
	require.NoError(t, err)

	require.EqualValues(t, d11, b11)
	require.Empty(t, b_b11)

	var b12 Duration
	err = d.QueryRow("SELECT `ttl` FROM `users` WHERE id=?", 12).Scan(&b12)
	require.NoError(t, err)

	require.EqualValues(t, d12, b12)

	var b13 Duration
	err = d.QueryRow("SELECT `ttl` FROM `users` WHERE id=?", 13).Scan(&b13)
	require.NoError(t, err)

	require.EqualValues(t, d13, b13)

}
