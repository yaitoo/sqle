package sqle

import (
	"context"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// TestExecContextHonorsCtx is the regression test for issue #59: the empty-args
// branches of Client.ExecContext and Tx.ExecContext used to silently substitute
// context.Background() for the caller-provided ctx, dropping cancellation and
// timeouts. After the fix, the ctx is propagated to the underlying driver.
//
// The test uses context.WithDeadline(time.Now().Add(-time.Second)) instead of
// WithCancel+immediate-cancel so the deadline is *deterministically* in the
// past — there is no race window where the driver could complete a fast
// in-memory SQLite operation before observing ctx.Done().
//
// Both the args == 0 branch (no prepared stmt) and the args > 0 branch (uses a
// cached stmt) are exercised. The args > 0 case primes the stmt cache with a
// healthy ctx first; a subsequent call with the expired ctx exercises the
// inner stmt.ExecContext(ctx, args...) line, which is where the original bug
// lived.
func TestExecContextHonorsCtx(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `exec_ctx_cancel` (`id` INT)")
	require.NoError(t, err)

	db := Open(d)

	// Deterministic, already-expired ctx. Same value is used across all sub-tests
	// to keep the table small and the intent obvious.
	expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	cancel()

	healthyCtx := context.Background()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			// Pre-fix: Client.ExecContext(expiredCtx, query) forwarded
			// context.Background() to db.DB.ExecContext, so the deadline
			// was dropped and the INSERT succeeded.
			name: "client_exec_no_args",
			run: func(t *testing.T) {
				_, err := db.ExecContext(expiredCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (1)")
				require.Error(t, err, "Client.ExecContext must surface ctx when len(args) == 0")
			},
		},
		{
			// Pre-fix: the inner stmt.ExecContext(ctx, args...) line
			// already used ctx correctly in Client, so this case only
			// catches a future regression that swaps it back to BG.
			name: "client_exec_with_args",
			run: func(t *testing.T) {
				// Prime the cache so the second call hits the cached path.
				_, err := db.ExecContext(healthyCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (?)", 10)
				require.NoError(t, err)

				_, err = db.ExecContext(expiredCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (?)", 11)
				require.Error(t, err, "Client.ExecContext must surface ctx via cached stmt.ExecContext")
			},
		},
		{
			name: "tx_exec_no_args",
			run: func(t *testing.T) {
				tx, err := db.Begin(nil)
				require.NoError(t, err)
				defer tx.Rollback() //nolint: errcheck

				_, err = tx.ExecContext(expiredCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (2)")
				require.Error(t, err, "Tx.ExecContext must surface ctx when len(args) == 0")
			},
		},
		{
			// Pre-fix: Tx.ExecContext's args > 0 branch also used ctx
			// correctly, so this case is forward-looking regression
			// coverage for the inner stmt.ExecContext line.
			name: "tx_exec_with_args",
			run: func(t *testing.T) {
				tx, err := db.Begin(nil)
				require.NoError(t, err)
				defer tx.Rollback() //nolint: errcheck

				_, err = tx.ExecContext(healthyCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (?)", 20)
				require.NoError(t, err)

				_, err = tx.ExecContext(expiredCtx, "INSERT INTO `exec_ctx_cancel` (`id`) VALUES (?)", 21)
				require.Error(t, err, "Tx.ExecContext must surface ctx via cached stmt.ExecContext")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t)
		})
	}
}
