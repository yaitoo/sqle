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

				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 1, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 2, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 3, "3@mail.com")

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
				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 11, "1@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 12, "2@mail.com")

					return err
				}, nil)

				dtc.Prepare(db.dbs[0], func(ctx context.Context, c Connector) error {
					_, err := c.Exec("INSERT INTO `dtc_1`(`id`,`email`) VALUES(?,?)", 13)

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
