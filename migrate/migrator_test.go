package migrate

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/shardid"
)

func createSqlite3() (*sql.DB, func(), error) {
	f, err := os.CreateTemp(".", "*.db")
	f.Close()

	clean := func() {
		os.Remove(f.Name()) //nolint
	}

	if err != nil {
		return nil, clean, err
	}

	db, err := sql.Open("sqlite3", f.Name())

	if err != nil {
		return nil, clean, err
	}

	return db, clean, nil

}

func TestDiscover(t *testing.T) {

	tests := []struct {
		name    string
		fsys    fs.FS
		options []Option
		setup   func(db *sql.DB) *Migrator
		assert  func(m *Migrator, t *testing.T)
		err     error
	}{
		{
			name: "semver_should_work",
			fsys: fstest.MapFS{
				"1.1.2/1_create_table_members.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE members"),
				},
				"0.1.0/1_create_table_users.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE users"),
				},
				"0.1.0/13_create_table_orders.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE orders"),
				},
				"0.1.0/02_create_table_login.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE logins"),
				},
			},
			setup: func(db *sql.DB) *Migrator {
				return New(sqle.Open(db))
			},
			assert: func(m *Migrator, t *testing.T) {
				require.Len(t, m.Versions, 2)
				v0_1_0 := m.Versions[0]
				require.Equal(t, 0, v0_1_0.Major)
				require.Equal(t, 1, v0_1_0.Minor)
				require.Equal(t, 0, v0_1_0.Patch)

				require.Len(t, v0_1_0.Migrations, 3)
				require.Equal(t, 1, v0_1_0.Migrations[0].Rank)
				require.Equal(t, "create_table_users", v0_1_0.Migrations[0].Name)
				require.Equal(t, "CREATE TABLE users", v0_1_0.Migrations[0].Scripts)
				require.Equal(t, 2, v0_1_0.Migrations[1].Rank)
				require.Equal(t, "create_table_login", v0_1_0.Migrations[1].Name)
				require.Equal(t, "CREATE TABLE logins", v0_1_0.Migrations[1].Scripts)
				require.Equal(t, 13, v0_1_0.Migrations[2].Rank)
				require.Equal(t, "create_table_orders", v0_1_0.Migrations[2].Name)
				require.Equal(t, "CREATE TABLE orders", v0_1_0.Migrations[2].Scripts)

				v1_1_2 := m.Versions[1]
				require.Len(t, v1_1_2.Migrations, 1)
				require.Equal(t, 1, v1_1_2.Migrations[0].Rank)
				require.Equal(t, "create_table_members", v1_1_2.Migrations[0].Name)
				require.Equal(t, "CREATE TABLE members", v1_1_2.Migrations[0].Scripts)

			},
		},
		{
			name: "suffix_should_work",
			options: []Option{
				WithSuffix(".mysql"),
			},
			fsys: fstest.MapFS{
				"1.1.2/1_create_table_members.mysql": &fstest.MapFile{
					Data: []byte("CREATE TABLE members"),
				},
				"0.1.0/1_create_table_users.mysql": &fstest.MapFile{
					Data: []byte("CREATE TABLE users"),
				},
				"0.1.0/13_create_table_orders.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE orders"),
				},
				"0.1.0/02_create_table_login.mysql": &fstest.MapFile{
					Data: []byte("CREATE TABLE logins"),
				},
			},
			setup: func(db *sql.DB) *Migrator {
				return New(sqle.Open(db))
			},
			assert: func(m *Migrator, t *testing.T) {
				require.Len(t, m.Versions, 2)
				v0_1_0 := m.Versions[0]
				require.Equal(t, 0, v0_1_0.Major)
				require.Equal(t, 1, v0_1_0.Minor)
				require.Equal(t, 0, v0_1_0.Patch)

				require.Len(t, v0_1_0.Migrations, 2)
				require.Equal(t, 1, v0_1_0.Migrations[0].Rank)
				require.Equal(t, "create_table_users", v0_1_0.Migrations[0].Name)
				require.Equal(t, "CREATE TABLE users", v0_1_0.Migrations[0].Scripts)
				require.Equal(t, 2, v0_1_0.Migrations[1].Rank)
				require.Equal(t, "create_table_login", v0_1_0.Migrations[1].Name)
				require.Equal(t, "CREATE TABLE logins", v0_1_0.Migrations[1].Scripts)

				v1_1_2 := m.Versions[1]
				require.Len(t, v1_1_2.Migrations, 1)
				require.Equal(t, 1, v1_1_2.Migrations[0].Rank)
				require.Equal(t, "create_table_members", v1_1_2.Migrations[0].Name)
				require.Equal(t, "CREATE TABLE members", v1_1_2.Migrations[0].Scripts)

			},
		},
		{
			name: "rotate_marks_should_be_parsed",
			fsys: fstest.MapFS{
				"1.1.2/1_create_table_members.sql": &fstest.MapFile{
					Data: []byte(`/* rotate:monthly=20240201-20240401 */
CREATE TABLE members`),
				},
				"0.1.0/1_create_table_users.sql": &fstest.MapFile{
					Data: []byte(`/* rotate:weekly=20240201-20240401 */
CREATE TABLE users`),
				},
				"0.1.0/13_create_table_orders.sql": &fstest.MapFile{
					Data: []byte(`/* rotate:daily=20240201-20240401 */
CREATE TABLE orders`),
				},
				"0.1.0/02_create_table_login.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE logins"),
				},
			},
			setup: func(db *sql.DB) *Migrator {
				return New(sqle.Open(db))
			},
			assert: func(m *Migrator, t *testing.T) {

				begin := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
				end := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
				require.Len(t, m.Versions, 2)
				v0_1_0 := m.Versions[0]
				require.Equal(t, 0, v0_1_0.Major)
				require.Equal(t, 1, v0_1_0.Minor)
				require.Equal(t, 0, v0_1_0.Patch)

				require.Len(t, v0_1_0.Migrations, 3)

				users := v0_1_0.Migrations[0]

				require.Equal(t, 1, users.Rank)
				require.Equal(t, "create_table_users", users.Name)
				require.Equal(t, `/* rotate:weekly=20240201-20240401 */
CREATE TABLE users`, users.Scripts)
				require.Equal(t, shardid.WeeklyRotate, users.Rotate)
				require.Equal(t, begin, users.RotateBegin)
				require.Equal(t, end, users.RotateEnd)

				logins := v0_1_0.Migrations[1]
				require.Equal(t, 2, logins.Rank)
				require.Equal(t, "create_table_login", logins.Name)
				require.Equal(t, "CREATE TABLE logins", logins.Scripts)
				require.Equal(t, shardid.NoRotate, logins.Rotate)

				orders := v0_1_0.Migrations[2]
				require.Equal(t, 13, orders.Rank)
				require.Equal(t, "create_table_orders", orders.Name)
				require.Equal(t, `/* rotate:daily=20240201-20240401 */
CREATE TABLE orders`, orders.Scripts)
				require.Equal(t, shardid.DailyRotate, orders.Rotate)
				require.Equal(t, begin, orders.RotateBegin)
				require.Equal(t, end, orders.RotateEnd)

				v1_1_2 := m.Versions[1]
				require.Len(t, v1_1_2.Migrations, 1)

				members := v1_1_2.Migrations[0]
				require.Equal(t, 1, members.Rank)
				require.Equal(t, "create_table_members", members.Name)
				require.Equal(t, `/* rotate:monthly=20240201-20240401 */
CREATE TABLE members`, members.Scripts)
				require.Equal(t, shardid.MonthlyRotate, members.Rotate)
				require.Equal(t, begin, members.RotateBegin)
				require.Equal(t, end, members.RotateEnd)

			},
		},
		{
			name: "rotation_should_work",
			fsys: fstest.MapFS{
				"monthly/members.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE members<rotate>`),
				},
				"monthly/users.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE users<rotate>`),
				},
				"monthly/invalid_users.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE users`),
				},
				"weekly/orders.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE orders<rotate>`),
				},
				"weekly/invalid_orders.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE orders`),
				},
				"daily/login.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE login<rotate>"),
				},
				"daily/invalid_login.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE login"),
				},
				"daily/logs.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE logs<rotate>"),
				},
			},
			setup: func(db *sql.DB) *Migrator {
				return New(sqle.Open(db))
			},
			assert: func(m *Migrator, t *testing.T) {
				require.Len(t, m.MonthlyRotations, 2)
				require.Equal(t, "members", m.MonthlyRotations[0].Name)
				require.Equal(t, "CREATE TABLE members<rotate>", m.MonthlyRotations[0].Script)
				require.Equal(t, "users", m.MonthlyRotations[1].Name)
				require.Equal(t, "CREATE TABLE users<rotate>", m.MonthlyRotations[1].Script)

				require.Len(t, m.WeeklyRotations, 1)
				require.Equal(t, "orders", m.WeeklyRotations[0].Name)
				require.Equal(t, "CREATE TABLE orders<rotate>", m.WeeklyRotations[0].Script)

				require.Len(t, m.DailyRotations, 2)
				require.Equal(t, "login", m.DailyRotations[0].Name)
				require.Equal(t, "CREATE TABLE login<rotate>", m.DailyRotations[0].Script)
				require.Equal(t, "logs", m.DailyRotations[1].Name)
				require.Equal(t, "CREATE TABLE logs<rotate>", m.DailyRotations[1].Script)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			db, clean, err := createSqlite3()
			defer clean()
			require.NoError(t, err)

			m := test.setup(db)

			err = m.Discover(test.fsys, test.options...)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.err)
			}
			test.assert(m, t)
		})
	}

}

func TestMigrate(t *testing.T) {

	tests := []struct {
		name   string
		setup  func(db *sql.DB) (*Migrator, error)
		assert func(t *testing.T, m *Migrator)
	}{
		{
			name: "single_command_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_table_members.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS members (
							id int NOT NULL,
							status tinyint NOT NULL DEFAULT '1',
							email varchar(50) NOT NULL,
							passwd varchar(120) NOT NULL,
							salt varchar(45) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
					"0.1.0/2_create_table_roles.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS roles (
						id int NOT NULL,
						name varchar(45) NOT NULL,
						PRIMARY KEY (id)
						);`),
					},
				}, WithModule("tests"))

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				type Member struct {
					ID     int
					Status int
					Email  string
					Passwd string
					Salt   string
				}

				err := m.dbs[0].QueryRow("SELECT id,status,email,passwd,salt FROM members WHERE id=?", 0).Bind(&Member{})
				require.ErrorIs(t, err, sql.ErrNoRows)

				type Role struct {
					ID   int
					Name string
				}

				err = m.dbs[0].QueryRow("SELECT id,name FROM roles WHERE id=?", 0).Bind(&Role{})
				require.ErrorIs(t, err, sql.ErrNoRows)

			},
		},
		{
			name: "multiple_commands_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/2_create_table_members_and_roles.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS roles (
						id int NOT NULL,
						name varchar(45) NOT NULL,
						PRIMARY KEY (id)
						);
						
						CREATE TABLE IF NOT EXISTS members (
							id int NOT NULL,
							status tinyint NOT NULL DEFAULT '1',
							email varchar(50) NOT NULL,
							passwd varchar(120) NOT NULL,
							salt varchar(45) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				type Member struct {
					ID     int
					Status int
					Email  string
					Passwd string
					Salt   string
				}

				err := m.dbs[0].QueryRow("SELECT id,status,email,passwd,salt FROM members WHERE id=?", 0).Bind(&Member{})
				require.ErrorIs(t, err, sql.ErrNoRows)

				type Role struct {
					ID   int
					Name string
				}

				err = m.dbs[0].QueryRow("SELECT id,name FROM roles WHERE id=?", 0).Bind(&Role{})
				require.ErrorIs(t, err, sql.ErrNoRows)

			},
		},
		{
			name: "with_monthly_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_monthly_logs.sql": &fstest.MapFile{
						Data: []byte(`/* rotate: monthly = 20240201 - 20240401 */
						CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"", "_202402", "_202403", "_202404",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM monthly_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
		{
			name: "with_weekly_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_weekly_logs.sql": &fstest.MapFile{
						Data: []byte(`/* rotate: weekly = 20240201 - 20240222 */
						CREATE TABLE IF NOT EXISTS weekly_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"", "_2024005", "_2024006", "_2024007", "_2024008",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM weekly_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
		{
			name: "with_daily_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_weekly_logs.sql": &fstest.MapFile{
						Data: []byte(`/* rotate: daily = 20240201 - 20240206 */
						CREATE TABLE IF NOT EXISTS daily_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"", "_20240201", "_20240202", "_20240203", "_20240204", "_20240205", "_20240206",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM daily_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
		{
			name: "with_invalid_rotate_should_be_skipped",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_invalid_logs.sql": &fstest.MapFile{
						Data: []byte(`/* no: daily = 20240201 - 20240206 */
						CREATE TABLE IF NOT EXISTS no_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"_20240201", "_20240202", "_20240203", "_20240204", "_20240205", "_20240206",
				}

				err := m.dbs[0].QueryRow("SELECT id FROM no_logs WHERE id=?", 0).Scan(&id)
				require.ErrorIs(t, err, sql.ErrNoRows)

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM daily_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorContains(t, err, "no such table")
				}

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, clean, err := createSqlite3()
			defer clean()
			require.NoError(t, err)

			m, err := test.setup(db)
			require.NoError(t, err)
			err = m.Init(context.TODO())
			require.NoError(t, err)
			err = m.Migrate(context.TODO())
			require.NoError(t, err)
			// Nothing should be changed on re-migration
			err = m.Migrate(context.TODO())
			require.NoError(t, err)

			test.assert(t, m)
		})
	}

}

func TestRotate(t *testing.T) {

	tests := []struct {
		name   string
		setup  func(db *sql.DB) (*Migrator, error)
		assert func(t *testing.T, m *Migrator)
	}{
		{
			name: "monthly_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))
				m.now = func() time.Time {
					return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
				}

				err := m.Discover(fstest.MapFS{
					"monthly/monthly_logs.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"_202402", "_202403",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM monthly_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
		{
			name: "weekly_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))
				m.now = func() time.Time {
					return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
				}

				err := m.Discover(fstest.MapFS{
					"weekly/weekly_logs.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS weekly_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"_2024005", "_2024006",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM weekly_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
		{
			name: "daily_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))
				m.now = func() time.Time {
					return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
				}

				err := m.Discover(fstest.MapFS{
					"daily/daily_logs.sql": &fstest.MapFile{
						Data: []byte(`CREATE TABLE IF NOT EXISTS daily_logs<rotate> (
							id int NOT NULL,
							msg varchar(50) NOT NULL,
							PRIMARY KEY (id)
						);`),
					},
				})

				if err != nil {
					return nil, err
				}

				return m, nil

			},
			assert: func(t *testing.T, m *Migrator) {
				var id int64

				rotations := []string{
					"_20240201", "_20240202",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM daily_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, clean, err := createSqlite3()
			defer clean()
			require.NoError(t, err)

			m, err := test.setup(db)
			require.NoError(t, err)
			err = m.Init(context.TODO())
			require.NoError(t, err)
			err = m.Rotate(context.TODO())
			require.NoError(t, err)
			// Nothing should be changed on re-rotated
			err = m.Rotate(context.TODO())
			require.NoError(t, err)

			test.assert(t, m)
		})
	}

}
