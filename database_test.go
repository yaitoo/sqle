package sqle

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	_ "github.com/mattn/go-sqlite3"
	"github.com/yaitoo/sqle/shardid"
)

// wrapperDB is a thin delegation wrapper around *sql.DB used to verify
// that custom implementations of Database are accepted by Open.
type wrapperDB struct {
	*sql.DB
}

// TestOpenGenericSlice verifies that the generic Open signature accepts
// a []*sql.DB slice directly (no manual conversion needed) thanks to
// the type parameter constrained by Database.
func TestOpenGenericSlice(t *testing.T) {
	dbs := []*sql.DB{createSQLite3(), createSQLite3()}

	db := Open(dbs...)
	require.NotNil(t, db)
	require.Equal(t, 0, db.On(shardid.ID{DatabaseID: 0}).Index)
}

// TestDatabaseWrapper proves that a custom type satisfying Database
// (here a thin *sql.DB wrapper) plugs into Open without changes.
func TestDatabaseWrapper(t *testing.T) {
	raw := createSQLite3()
	raw.Exec("CREATE TABLE `t` (`v` int)") //nolint: errcheck

	wrapped := &wrapperDB{DB: raw}

	db := Open(wrapped)

	ctx := context.Background()

	// Exec via wrapper
	res, err := db.ExecContext(ctx, "INSERT INTO `t` (`v`) VALUES (?)", 42)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)

	// Query via wrapper
	var v int
	row := db.QueryRowContext(ctx, "SELECT `v` FROM `t` WHERE `v` = ?", 42)
	require.NoError(t, row.Scan(&v))
	require.Equal(t, 42, v)

	// Forwarded methods still work
	require.NotNil(t, db.Stats())
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	require.NoError(t, db.PingContext(ctx))
	require.NoError(t, db.Ping())
}