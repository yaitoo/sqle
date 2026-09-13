package sqle

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestClientExecContextHonorsCancelledCtx is a regression test for issue #59:
// Client.ExecContext used to drop the caller-provided ctx and pass
// context.Background() to the underlying *sql.DB.ExecContext when len(args) == 0.
// A pre-cancelled context must surface as an error in both the args > 0 and
// the args == 0 branches. Without the fix, the args == 0 path silently swaps
// ctx for context.Background() and the cancellation is lost.
func TestClientExecContextHonorsCancelledCtx(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `client_exec_ctx_cancel` (`id` INT)")
	require.NoError(t, err)

	db := Open(d)

	t.Run("no_args_branch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := db.ExecContext(ctx, "INSERT INTO `client_exec_ctx_cancel` (`id`) VALUES (1)")
		require.Error(t, err, "Client.ExecContext must surface ctx cancellation when len(args) == 0")
	})

	t.Run("args_branch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := db.ExecContext(ctx, "INSERT INTO `client_exec_ctx_cancel` (`id`) VALUES (?)", 2)
		require.Error(t, err, "Client.ExecContext must surface ctx cancellation when len(args) > 0")
	})
}

// TestTxExecContextHonorsCancelledCtx is the matching regression test for
// Tx.ExecContext at tx.go:146. Same shape as the Client test: a pre-cancelled
// ctx must surface as an error in both branches.
func TestTxExecContextHonorsCancelledCtx(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `tx_exec_ctx_cancel` (`id` INT)")
	require.NoError(t, err)

	db := Open(d)

	t.Run("no_args_branch", func(t *testing.T) {
		tx, err := db.Begin(nil)
		require.NoError(t, err)
		defer tx.Rollback() //nolint: errcheck

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = tx.ExecContext(ctx, "INSERT INTO `tx_exec_ctx_cancel` (`id`) VALUES (1)")
		require.Error(t, err, "Tx.ExecContext must surface ctx cancellation when len(args) == 0")
	})

	t.Run("args_branch", func(t *testing.T) {
		tx, err := db.Begin(nil)
		require.NoError(t, err)
		defer tx.Rollback() //nolint: errcheck

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = tx.ExecContext(ctx, "INSERT INTO `tx_exec_ctx_cancel` (`id`) VALUES (?)", 2)
		require.Error(t, err, "Tx.ExecContext must surface ctx cancellation when len(args) > 0")
	})
}

// TestClientExecContextSuccessAfterFix sanity-checks the fix doesn't break the
// happy path: ExecContext with no args and a non-cancelled ctx must succeed.
func TestClientExecContextSuccessAfterFix(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `client_exec_ctx_ok` (`id` INT)")
	require.NoError(t, err)

	db := Open(d)

	t.Run("no_args_branch", func(t *testing.T) {
		_, err := db.ExecContext(context.Background(), "INSERT INTO `client_exec_ctx_ok` (`id`) VALUES (1)")
		require.NoError(t, err)
	})

	t.Run("args_branch", func(t *testing.T) {
		_, err := db.ExecContext(context.Background(), "INSERT INTO `client_exec_ctx_ok` (`id`) VALUES (?)", 2)
		require.NoError(t, err)
	})
}

// TestTxExecContextSuccessAfterFix is the matching happy-path test for Tx.
func TestTxExecContextSuccessAfterFix(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `tx_exec_ctx_ok` (`id` INT)")
	require.NoError(t, err)

	db := Open(d)

	t.Run("no_args_branch", func(t *testing.T) {
		tx, err := db.Begin(nil)
		require.NoError(t, err)
		defer tx.Rollback() //nolint: errcheck

		_, err = tx.ExecContext(context.Background(), "INSERT INTO `tx_exec_ctx_ok` (`id`) VALUES (1)")
		require.NoError(t, err)
	})

	t.Run("args_branch", func(t *testing.T) {
		tx, err := db.Begin(nil)
		require.NoError(t, err)
		defer tx.Rollback() //nolint: errcheck

		_, err = tx.ExecContext(context.Background(), "INSERT INTO `tx_exec_ctx_ok` (`id`) VALUES (?)", 2)
		require.NoError(t, err)
	})
}
