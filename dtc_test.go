package sqle

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDTCWithDB(t *testing.T) {
	os.Remove("dtc_db.db")

	d, err := sql.Open("sqlite3", "file:dtc_1.db?cache=shared&mode=rwc")
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

				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 1, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
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
				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 11, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 12, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, conn Connector) error {
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

				dtc.Prepare(db1.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1` (`id`,`email`) VALUES(?,?)", 1, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db2.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_2` (`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db3.dbs[0], func(ctx context.Context, conn Connector) error {
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
				dtc.Prepare(db1.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 11, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db2.dbs[0], func(ctx context.Context, conn Connector) error {
					_, err := conn.Exec("INSERT INTO `dtc_2`(`id`,`email`) VALUES(?,?)", 12, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db3.dbs[0], func(ctx context.Context, conn Connector) error {
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

	dtc.Prepare(db1.dbs[0], func(ctx context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_1` (`id`,`email`) VALUES(?,?)", 1, "1@mail.com")
		return err
	}, func(ctx context.Context, c Connector) error {
		_, err := c.Exec("DELETE FROM `dtc_1` WHERE id=?", 1)
		return err
	})

	dtc.Prepare(db2.dbs[0], func(ctx context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_2` (`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

		return err
	}, func(ctx context.Context, c Connector) error {
		_, err := c.Exec("DELETE FROM `dtc_2` WHERE id=?", 2)
		return err
	})

	dtc.Prepare(db3.dbs[0], func(ctx context.Context, conn Connector) error {
		_, err := conn.Exec("INSERT INTO `dtc_3` (`id`,`email`) VALUES(?,?)", 3, "3@mail.com")

		return err
	}, func(ctx context.Context, c Connector) error {
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
