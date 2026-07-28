package sqle

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle/shardid"
)

// customTx is a transaction wrapper that counts Commit/Rollback calls
// so we can verify that custom Tx implementations flow through the
// sqle.TxContext wrapper correctly.
type customTx struct {
	*sql.Tx
	commitCalls   atomic.Int32
	rollbackCalls atomic.Int32
}

func (tx *customTx) Commit() error {
	tx.commitCalls.Add(1)
	return tx.Tx.Commit()
}

func (tx *customTx) Rollback() error {
	tx.rollbackCalls.Add(1)
	return tx.Tx.Rollback()
}

// customDB is a Database implementation that returns a customTx from
// Begin/BeginTx. It satisfies the Database interface by overriding
// Begin/BeginTx (the rest of the methods are promoted from *sql.DB).
type customDB struct {
	*sql.DB
}

func (db *customDB) Begin() (Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func (db *customDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := db.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &customTx{Tx: tx}, nil
}

// TestCustomTxCommitHook proves that a custom Tx implementation
// (here, customTx) reaches Commit/Rollback when used through
// *sqle.TxContext.
func TestCustomTxCommitHook(t *testing.T) {
	raw := createSQLite3()
	_, err := raw.Exec("CREATE TABLE `t` (`v` int)")
	require.NoError(t, err)

	wrapped := &customDB{DB: raw}
	db := Open(wrapped)

	tx, err := db.Begin(nil)
	require.NoError(t, err)

	// Reach through TxContext.Tx to grab the underlying customTx so we
	// can assert on the hook counters.
	custom, ok := tx.Tx.(*customTx)
	require.True(t, ok, "expected *customTx, got %T", tx.Tx)
	require.Equal(t, int32(0), custom.commitCalls.Load())

	_, err = tx.Exec("INSERT INTO `t` (`v`) VALUES (1)")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	require.Equal(t, int32(1), custom.commitCalls.Load(),
		"customTx.Commit hook should fire through TxContext.Commit")

	// Pull the row back to verify the commit went through.
	var v int
	row := db.QueryRow("SELECT `v` FROM `t` WHERE `v` = 1")
	require.NoError(t, row.Scan(&v))
	require.Equal(t, 1, v)
}

// TestCustomTxRollbackHook proves that Rollback on the underlying
// customTx is invoked when Transaction falls back to rollback.
func TestCustomTxRollbackHook(t *testing.T) {
	raw := createSQLite3()
	_, err := raw.Exec("CREATE TABLE `t` (`v` int)")
	require.NoError(t, err)

	wrapped := &customDB{DB: raw}
	db := Open(wrapped)

	var underlying *customTx
	sentinel := errors.New("nope")
	err = db.Transaction(context.Background(), nil, func(ctx context.Context, tx *TxContext) error {
		// Capture the underlying customTx for the post-transaction
		// assertion on rollbackCalls.
		custom, ok := tx.Tx.(*customTx)
		require.True(t, ok, "expected *customTx, got %T", tx.Tx)
		underlying = custom
		require.Equal(t, int32(0), underlying.rollbackCalls.Load())

		_, err := tx.Exec("INSERT INTO `t` (`v`) VALUES (1)")
		require.NoError(t, err)
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	require.NotNil(t, underlying)
	require.Equal(t, int32(1), underlying.rollbackCalls.Load(),
		"customTx.Rollback hook should fire when Transaction aborts")

	// The row should not be visible (transaction rolled back).
	var count int
	row := db.QueryRow("SELECT COUNT(*) FROM `t`")
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)
}

// TestCustomTxBeginReturnsInterface proves that Client.BeginTx returns
// the *TxContext wrapper regardless of the underlying Tx concrete type.
func TestCustomTxBeginReturnsInterface(t *testing.T) {
	raw := createSQLite3()
	wrapped := &customDB{DB: raw}
	db := Open(wrapped)

	ctx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = ctx.Rollback() }()

	// TxContext wraps the Tx interface, so the embedded Tx
	// must be the customTx instance returned by customDB.BeginTx.
	custom, ok := ctx.Tx.(*customTx)
	require.True(t, ok, "expected *customTx, got %T", ctx.Tx)
	require.NotNil(t, custom)
}

// TestOpenDBBackwardCompat makes sure OpenDB (the *sql.DB convenience
// wrapper) still works for the bare-*sql.DB use case.
func TestOpenDBBackwardCompat(t *testing.T) {
	dbs := []*sql.DB{createSQLite3(), createSQLite3()}
	db := OpenDB(dbs...)
	require.NotNil(t, db)
	require.Equal(t, 0, db.On(shardid.ID{DatabaseID: 0}).Index)

	// Round-trip a row through the wrapped database.
	_, err := db.ExecContext(context.Background(), "CREATE TABLE `t` (`v` int)")
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), "INSERT INTO `t` (`v`) VALUES (1)")
	require.NoError(t, err)
}

// TestOpenMixedArgs shows that Open accepts both *sql.DB and Database
// implementations in the same call.
func TestOpenMixedArgs(t *testing.T) {
	raw := createSQLite3()
	wrapped := &wrapperDB{DB: raw}

	// Mix raw *sql.DB and a custom Database wrapper.
	db := Open(raw, wrapped)
	require.NotNil(t, db)
	require.Equal(t, 0, db.On(shardid.ID{DatabaseID: 0}).Index)
	require.Equal(t, 1, db.dbs[1].Index)
}
