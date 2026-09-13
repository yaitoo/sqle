package sqle

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDTCWithDB(t *testing.T) {
	os.Remove("dtc_1.db")

	d, err := sql.Open("sqlite3", "file:dtc_1.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d.Exec("DROP TABLE IF EXISTS `dtc_1`")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `dtc_1` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	db := Open(d)

	var tests = []struct {
		name   string
		setup  func() *DTC
		assert func(ra *require.Assertions)
	}{
		{
			name: "multiple_txs_commit_should_work",
			setup: func() *DTC {
				dtc := NewDTC(context.Background(), nil)

				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 1, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 3, "3@mail.com")

					return err
				}, nil)

				return dtc
			},
			assert: func(ra *require.Assertions) {
				var id int
				err := db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 1).Scan(&id)
				ra.NoError(err)
				ra.Equal(1, id)

				err = db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 2).Scan(&id)
				ra.NoError(err)
				ra.Equal(2, id)

				err = db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 3).Scan(&id)
				ra.NoError(err)
				ra.Equal(3, id)
			},
		},
		{
			name: "multiple_txs_rollback_should_work",
			setup: func() *DTC {
				dtc := NewDTC(context.Background(), nil)
				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 11, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 12, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 13)

					return err
				}, nil)

				return dtc
			},
			assert: func(ra *require.Assertions) {
				var id int
				err := db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 11).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)

				err = db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 12).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)

				err = db.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 13).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dtc := test.setup()

			err := dtc.Commit()
			if err != nil {
				dtc.Rollback()
			}

			test.assert(require.New(t))
		})
	}
}

func TestDTCWithDBs(t *testing.T) {
	os.Remove("dtc_dbs_1.db")
	os.Remove("dtc_dbs_2.db")
	os.Remove("dtc_dbs_3.db")

	d1, err := sql.Open("sqlite3", "file:dtc_dbs_1.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d1.Exec("CREATE TABLE `dtc_1` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	d2, err := sql.Open("sqlite3", "file:dtc_dbs_2.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d2.Exec("CREATE TABLE `dtc_2` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	d3, err := sql.Open("sqlite3", "file:dtc_dbs_3.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d3.Exec("CREATE TABLE `dtc_3` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	db1 := Open(d1)
	db2 := Open(d2)
	db3 := Open(d3)

	var tests = []struct {
		name   string
		setup  func() *DTC
		assert func(ra *require.Assertions)
	}{
		{
			name: "multiple_txs_commit_should_work",
			setup: func() *DTC {
				dtc := NewDTC(context.Background(), nil)

				dtc.Prepare(db1.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1` (`id`,`email`) VALUES(?,?)", 1, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db2.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_2` (`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db3.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_3` (`id`,`email`) VALUES(?,?)", 3, "3@mail.com")

					return err
				}, nil)

				return dtc
			},
			assert: func(ra *require.Assertions) {
				var id int
				err := db1.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 1).Scan(&id)
				ra.NoError(err)
				ra.Equal(1, id)

				err = db2.QueryRow("SELECT id FROM `dtc_2` WHERE id=?", 2).Scan(&id)
				ra.NoError(err)
				ra.Equal(2, id)

				err = db3.QueryRow("SELECT id FROM `dtc_3` WHERE id=?", 3).Scan(&id)
				ra.NoError(err)
				ra.Equal(3, id)
			},
		},
		{
			name: "multiple_txs_rollback_should_work",
			setup: func() *DTC {
				dtc := NewDTC(context.Background(), nil)
				dtc.Prepare(db1.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 11, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db2.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_2`(`id`,`email`) VALUES(?,?)", 12, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db3.dbs[0], func(_ context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_3`(`id`,`email`) VALUES(?,?)", 13)

					return err
				}, nil)

				return dtc
			},
			assert: func(ra *require.Assertions) {
				var id int
				err := db1.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 11).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)

				err = db2.QueryRow("SELECT id FROM `dtc_2` WHERE id=?", 12).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)

				err = db3.QueryRow("SELECT id FROM `dtc_3` WHERE id=?", 13).Scan(&id)
				ra.ErrorIs(err, sql.ErrNoRows)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dtc := test.setup()

			err := dtc.Commit()
			if err != nil {
				dtc.Rollback()
			}

			test.assert(require.New(t))
		})
	}
}

func TestDTCRevert(t *testing.T) {
	os.Remove("dtc_revert_1.db")
	os.Remove("dtc_revert_2.db")
	os.Remove("dtc_revert_3.db")

	d1, err := sql.Open("sqlite3", "file:dtc_revert_1.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d1.Exec("CREATE TABLE `dtc_1` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	d2, err := sql.Open("sqlite3", "file:dtc_revert_2.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d2.Exec("CREATE TABLE `dtc_2` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	d3, err := sql.Open("sqlite3", "file:dtc_revert_3.db?cache=shared&mode=rwc")
	require.NoError(t, err)

	_, err = d3.Exec("CREATE TABLE `dtc_3` (`id` int , `email` varchar(50),`created_at` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	db1 := Open(d1)
	db2 := Open(d2)
	db3 := Open(d3)

	dtc := NewDTC(context.Background(), nil)

	dtc.Prepare(db1.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_1` (`id`,`email`) VALUES(?,?)", 1, "1@mail.com")
		return err
	}, func(_ context.Context, c Connector) error {
		_, err := c.Exec("DELETE FROM `dtc_1` WHERE id=?", 1)
		return err
	})

	dtc.Prepare(db2.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_2` (`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

		return err
	}, func(_ context.Context, c Connector) error {
		_, err := c.Exec("DELETE FROM `dtc_2` WHERE id=?", 2)
		return err
	})

	dtc.Prepare(db3.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_3` (`id`,`email`) VALUES(?,?)", 3, "3@mail.com")

		return err
	}, func(_ context.Context, c Connector) error {
		_, err := c.Exec("DELETE FROM `dtc_3` WHERE id=?", 3)
		return err
	})

	ra := require.New(t)
	err = dtc.Commit()
	ra.NoError(err)

	errs := dtc.Rollback()
	ra.Len(errs, 0)

	var id int
	err = db1.QueryRow("SELECT id FROM `dtc_1` WHERE id=?", 11).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)

	err = db2.QueryRow("SELECT id FROM `dtc_2` WHERE id=?", 12).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)

	err = db3.QueryRow("SELECT id FROM `dtc_3` WHERE id=?", 13).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)

}

// TestDTCCommitRollbackOnPartialFailure verifies that when an exec callback
// fails midway through DTC.Commit, every session whose BeginTx succeeded
// earlier in the same Commit call is rolled back. Without this behavior
// each failed Commit would leak an *sql.Tx that holds a connection from the
// pool until the *sql.Tx finalizer runs at GC time.
func TestDTCCommitRollbackOnPartialFailure(t *testing.T) {
	_ = os.Remove("dtc_pf_a.db")
	_ = os.Remove("dtc_pf_b.db")

	dA, err := sql.Open("sqlite3", "file:dtc_pf_a.db?cache=shared&mode=rwc")
	require.NoError(t, err)
	_, err = dA.Exec("CREATE TABLE `dtc_pf_a` (`id` int PRIMARY KEY)")
	require.NoError(t, err)
	dbA := Open(dA)

	dB, err := sql.Open("sqlite3", "file:dtc_pf_b.db?cache=shared&mode=rwc")
	require.NoError(t, err)
	_, err = dB.Exec("CREATE TABLE `dtc_pf_b` (`id` int PRIMARY KEY)")
	require.NoError(t, err)
	dbB := Open(dB)

	// Capture stats before Commit so we can compare after.
	inUseBeforeA := dbA.dbs[0].Stats().InUse
	inUseBeforeB := dbB.dbs[0].Stats().InUse

	dtc := NewDTC(context.Background(), nil)

	// Session A: succeeds, holds a tx open until Commit finishes.
	dtc.Prepare(dbA.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_pf_a`(`id`) VALUES(?)", 1)
		return err
	}, nil)

	// Session B: exec fails (wrong number of args). Commit returns here.
	dtc.Prepare(dbB.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_pf_b`(`id`) VALUES(?)")
		return err
	}, nil)

	err = dtc.Commit()
	require.Error(t, err, "Commit should surface the exec error from session B")

	// The opened tx on session A must have been rolled back automatically;
	// otherwise its connection would still be in use.
	require.Equal(t, inUseBeforeA, dbA.dbs[0].Stats().InUse,
		"no connection should remain held by session A's tx after a failed Commit")
	require.Equal(t, inUseBeforeB, dbB.dbs[0].Stats().InUse,
		"no connection should remain held by session B's tx after a failed Commit")

	// Strengthen Issue 5: prove the defer rolled the txs back synchronously,
	// not lazily at GC time. Opening a fresh tx on the same pool must work
	// immediately; if the connection were still held, the pool's max-open
	// ceiling (default 0 = unlimited) would not block it, but we close and
	// reopen to force pool eviction if needed.
	txA, txErr := dbA.dbs[0].BeginTx(context.Background(), nil)
	require.NoError(t, txErr, "pool must release session A's tx synchronously after failed Commit")
	require.NoError(t, txA.Rollback())

	txB, txErr := dbB.dbs[0].BeginTx(context.Background(), nil)
	require.NoError(t, txErr, "pool must release session B's tx synchronously after failed Commit")
	require.NoError(t, txB.Rollback())

	// Following the documented caller pattern must not panic and must not
	// append spurious ErrTxDone noise: the defer already rolled back the
	// opened sessions, so Rollback should find nothing left to do for them.
	ra := require.New(t)
	errs := dtc.Rollback()
	ra.Empty(errs, "Rollback must not return ErrTxDone noise after the defer already rolled back")

	var id int
	err = dbA.QueryRow("SELECT id FROM `dtc_pf_a` WHERE id=?", 1).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)

	err = dbB.QueryRow("SELECT id FROM `dtc_pf_b` WHERE id=?", 1).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)
}

// TestDTCCommitRollbackOnBeginTxFailure verifies that when a later BeginTx
// fails, the sessions whose BeginTx already succeeded are rolled back so
// their connections return to the pool. BeginTx failures are simulated by
// closing the underlying *sql.DB before the DTC starts; this is the closest
// in-process approximation of a transient network blip.
func TestDTCCommitRollbackOnBeginTxFailure(t *testing.T) {
	_ = os.Remove("dtc_btf_a.db")
	_ = os.Remove("dtc_btf_b.db")

	dA, err := sql.Open("sqlite3", "file:dtc_btf_a.db?cache=shared&mode=rwc")
	require.NoError(t, err)
	_, err = dA.Exec("CREATE TABLE `dtc_btf_a` (`id` int PRIMARY KEY)")
	require.NoError(t, err)
	dbA := Open(dA)

	dB, err := sql.Open("sqlite3", "file:dtc_btf_b.db?cache=shared&mode=rwc")
	require.NoError(t, err)
	_, err = dB.Exec("CREATE TABLE `dtc_btf_b` (`id` int PRIMARY KEY)")
	require.NoError(t, err)
	dbB := Open(dB)

	inUseBeforeA := dbA.dbs[0].Stats().InUse

	// Close dbB so its next BeginTx fails — session B is unreachable.
	require.NoError(t, dB.Close())

	dtc := NewDTC(context.Background(), nil)

	dtc.Prepare(dbA.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_btf_a`(`id`) VALUES(?)", 1)
		return err
	}, nil)

	dtc.Prepare(dbB.dbs[0], func(_ context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_btf_b`(`id`) VALUES(?)", 1)
		return err
	}, nil)

	err = dtc.Commit()
	require.Error(t, err, "Commit should surface the BeginTx failure on session B")

	// Session A's tx must have been rolled back even though its own exec
	// never ran (BeginTx succeeded, then the loop moved on and failed on B).
	// This is the leak the fix targets: without the defer-rollback the
	// connection would remain in use until GC.
	require.Equal(t, inUseBeforeA, dbA.dbs[0].Stats().InUse,
		"session A's tx must be rolled back when a later BeginTx fails")

	// Following the documented caller pattern (commit then rollback on error)
	// must not panic: session B's BeginTx never returned a tx so its s.tx is
	// nil. Rollback must skip it rather than dereference.
	require.NotPanics(t, func() { _ = dtc.Rollback() })

	ra := require.New(t)
	var id int
	err = dbA.QueryRow("SELECT id FROM `dtc_btf_a` WHERE id=?", 1).Scan(&id)
	ra.ErrorIs(err, sql.ErrNoRows)
}
