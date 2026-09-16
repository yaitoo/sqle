package migrate

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"sync"
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
		{
			// Regression test for issue #65: a 'monthly'/'weekly'/'daily'
			// subdirectory nested inside a version directory must not be
			// loaded as a rotation source. Only direct children of the FS
			// root are rotation sources.
			//
			// This case has no top-level monthly/weekly/daily directories
			// so that the buggy code (which loaded the nested subdirs as
			// rotations) produces a non-empty rotation slice and the fixed
			// code produces an empty one.
			name: "nested_rotation_subdirs_should_be_ignored",
			fsys: fstest.MapFS{
				// A regular version directory. Its 'monthly'/'weekly'/'daily'
				// subfolders must NOT be treated as rotation sources even
				// though they share the reserved names.
				"0.0.1/1_create_table_users.sql": &fstest.MapFile{
					Data: []byte("CREATE TABLE users"),
				},
				"0.0.1/monthly/stale_monthly.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE stale_monthly<rotate>`),
				},
				"0.0.1/weekly/stale_weekly.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE stale_weekly<rotate>`),
				},
				"0.0.1/daily/stale_daily.sql": &fstest.MapFile{
					Data: []byte(`CREATE TABLE stale_daily<rotate>`),
				},
			},
			setup: func(db *sql.DB) *Migrator {
				return New(sqle.Open(db))
			},
			assert: func(m *Migrator, t *testing.T) {
				// No top-level rotation dir exists, so none should be loaded.
				require.Empty(t, m.MonthlyRotations)
				require.Empty(t, m.WeeklyRotations)
				require.Empty(t, m.DailyRotations)

				// The version directory itself is still discovered; its
				// nested rotation-named subdirs are simply ignored.
				require.Len(t, m.Versions, 1)
				require.Equal(t, "0.0.1", m.Versions[0].Name)
				require.Len(t, m.Versions[0].Migrations, 1)
				require.Equal(t, "create_table_users", m.Versions[0].Migrations[0].Name)
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
			name: "multiple_statements_with_rotate_should_work",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_with_rotate.sql": &fstest.MapFile{
						Data: []byte(`/* rotate: monthly = 20240201 - 20240301 */
						CREATE TABLE IF NOT EXISTS multi_logs<rotate> (
							id int NOT NULL,
							PRIMARY KEY (id)
						);
						CREATE TABLE IF NOT EXISTS multi_users<rotate> (
							id int NOT NULL,
							PRIMARY KEY (id)
						);
						CREATE INDEX IF NOT EXISTS idx_multi_logs<rotate> ON multi_logs<rotate>(id);`),
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
					"", "_202402", "_202403",
				}

				for _, rt := range rotations {
					err := m.dbs[0].QueryRow("SELECT id FROM multi_logs"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)

					err = m.dbs[0].QueryRow("SELECT id FROM multi_users"+rt+" WHERE id=?", 0).Scan(&id)
					require.ErrorIs(t, err, sql.ErrNoRows)
				}

				// Verify the rotated indexes were actually created. The
				// script declares `CREATE INDEX IF NOT EXISTS
				// idx_multi_logs<rotate> ON multi_logs<rotate>(id)` — the
				// <rotate> placeholder appears in both the index name and
				// the referenced table, so a regression in either
				// substitution would still leave the table query above
				// passing. Asserting sqlite_master catches that.
				for _, rt := range rotations {
					var name string
					err := m.dbs[0].QueryRow(
						"SELECT name FROM sqlite_master WHERE type='index' AND name=?",
						"idx_multi_logs"+rt,
					).Scan(&name)
					require.NoError(t, err, "expected idx_multi_logs%s to exist", rt)
					require.Equal(t, "idx_multi_logs"+rt, name)
				}

			},
		},
		{
			name: "semicolons_in_comments_should_not_split_statements",
			setup: func(db *sql.DB) (*Migrator, error) {

				m := New(sqle.Open(db))

				err := m.Discover(fstest.MapFS{
					"0.1.0/1_create_with_comments.sql": &fstest.MapFile{
						Data: []byte(`-- TODO: drop foo; keep bar;
CREATE TABLE IF NOT EXISTS foo (
	id int NOT NULL,
	PRIMARY KEY (id)
);
/* a block comment; with a semicolon */
CREATE TABLE IF NOT EXISTS bar (
	id int NOT NULL,
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
				err := m.dbs[0].QueryRow("SELECT id FROM foo WHERE id=?", 0).Bind(&struct {
					ID int
				}{})
				require.ErrorIs(t, err, sql.ErrNoRows)

				err = m.dbs[0].QueryRow("SELECT id FROM bar WHERE id=?", 0).Bind(&struct {
					ID int
				}{})
				require.ErrorIs(t, err, sql.ErrNoRows)
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

// Regression tests for issue #67: a non-ErrNoRows error from the
// sqle_migrations lookup must be reported as MigrationStatusUnknown,
// not MigrationStatusNew. Otherwise the caller treats it as a fresh
// migration and either logs a misleading status or, on a later run
// after the txn rolled back, re-executes a non-idempotent script.
func TestGetMigrationStatus_DBError(t *testing.T) {
	db, clean, err := createSqlite3()
	defer clean()
	require.NoError(t, err)

	// Deliberately do NOT call m.Init() — sqle_migrations does not exist,
	// so the first SELECT raises a "no such table" error. That is a real
	// DB error, indistinguishable in shape from a transient driver
	// timeout, and must not be classified as a new migration.
	sqleDB := sqle.Open(db)
	m := New(sqleDB)

	s := Migration{
		Name:     "create_table_users",
		Rank:     1,
		Checksum: "abc123",
		Scripts:  "CREATE TABLE users",
	}

	require.NoError(t, sqleDB.Transaction(context.TODO(), nil, func(ctx context.Context, tx *sqle.Tx) error {
		status, err := m.getMigrationStatus(tx, "0.0.1", s)
		require.Error(t, err, "getMigrationStatus must propagate the underlying DB error")
		require.Equal(t, MigrationStatusUnknown, status,
			"non-ErrNoRows errors must be MigrationStatusUnknown, not MigrationStatusNew (issue #67)")
		return nil
	}))
}

// Companion test for issue #67: when the migrator sees a DB error
// during getMigrationStatus, it must abort and propagate the error
// instead of silently reclassifying the script as new and running it.
func TestMigrate_DBError_AbortsAndPropagates(t *testing.T) {
	db, clean, err := createSqlite3()
	defer clean()
	require.NoError(t, err)

	m := New(sqle.Open(db))

	require.NoError(t, m.Discover(fstest.MapFS{
		"0.1.0/1_create_table_users.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS users (id int NOT NULL, PRIMARY KEY (id));`),
		},
	}, WithModule("tests")))

	// Do NOT call m.Init(). Migrate must abort and return the underlying
	// DB error, never silently run the migration as if it were new.
	err = m.Migrate(context.TODO())
	require.Error(t, err,
		"Migrate must propagate the DB error from getMigrationStatus instead of treating it as a new migration (issue #67)")

	// And it must NOT have executed the script — the table should not exist.
	var name string
	row := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", "users")
	require.ErrorIs(t, row.Scan(&name), sql.ErrNoRows,
		"the migration script must not be executed when the status check failed with a DB error")
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

// Regression test for issue #74: Migrator.startRotate reused the
// `checksum` variable across iterations of both the rotations loop
// and the rotatedNames loop. When a prior Scan had populated
// `checksum` (e.g. because a row for an earlier rotation already
// existed in sqle_rotations) and the next Scan returned
// sql.ErrNoRows, Scan does NOT overwrite the destination — so the
// stale value triggered the `if checksum != ""` short-circuit and
// the insert for the later rotation was silently skipped.
//
// This test reproduces the bug deterministically: it configures two
// monthly rotations (monthly_logs and monthly_users), pre-populates
// sqle_rotations only for the FIRST rotation, then calls Rotate.
// With the bug, Rotate would log "[✔]" for the second rotation and
// never create its tables or sqle_rotations rows. With the fix, the
// second rotation's tables and rows are created normally.
func TestRotate_ChecksumNotReusedAcrossIterations(t *testing.T) {
	db, clean, err := createSqlite3()
	defer clean()
	require.NoError(t, err)

	m := New(sqle.Open(db))
	m.now = func() time.Time {
		return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	}

	require.NoError(t, m.Discover(fstest.MapFS{
		"monthly/monthly_logs.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
				id int NOT NULL,
				msg varchar(50) NOT NULL,
				PRIMARY KEY (id)
			);`),
		},
		"monthly/monthly_users.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS monthly_users<rotate> (
				id int NOT NULL,
				name varchar(50) NOT NULL,
				PRIMARY KEY (id)
			);`),
		},
	}))

	require.NoError(t, m.Init(context.TODO()))

	require.Len(t, m.MonthlyRotations, 2)
	first := m.MonthlyRotations[0]
	second := m.MonthlyRotations[1]
	require.NotEqual(t, first.Checksum, second.Checksum,
		"the two rotations must have different checksums for this test to exercise the bug")

	// Pre-populate sqle_rotations ONLY for the first rotation's
	// rotated_names. This is the state in which the bug surfaces:
	// the first rotation's Scan succeeds (writing first.Checksum to
	// the shared `checksum` variable) and the second rotation's Scan
	// returns sql.ErrNoRows — without the fix, the stale value
	// short-circuits the insert.
	for _, rn := range []string{"_202402", "_202403"} {
		_, err := db.Exec(
			"INSERT INTO sqle_rotations(checksum, rotated_name, name, rotated_on, execution_time) VALUES (?, ?, ?, ?, ?)",
			first.Checksum, rn, first.Name, time.Now(), "0s",
		)
		require.NoError(t, err)
	}

	require.NoError(t, m.Rotate(context.TODO()))

	// First rotation: rows existed before Rotate, so they must still be
	// present and untouched (Rotate should have skipped them with [✔]).
	for _, rn := range []string{"_202402", "_202403"} {
		var name string
		err := m.dbs[0].QueryRow(
			"SELECT name FROM sqle_rotations WHERE checksum = ? AND rotated_name = ?",
			first.Checksum, rn,
		).Scan(&name)
		require.NoError(t, err, "sqle_rotations row for first rotation %s must exist", rn)
		require.Equal(t, first.Name, name)
	}

	// Second rotation: rows did NOT exist before Rotate. With the bug,
	// they are still missing because the stale `checksum` variable
	// short-circuits the insert. With the fix, they are inserted.
	for _, rn := range []string{"_202402", "_202403"} {
		var name string
		err := m.dbs[0].QueryRow(
			"SELECT name FROM sqle_rotations WHERE checksum = ? AND rotated_name = ?",
			second.Checksum, rn,
		).Scan(&name)
		require.NoError(t, err,
			"sqle_rotations row for second rotation %s must exist after Rotate (issue #74)", rn)
		require.Equal(t, second.Name, name)
	}

	// And the actual rotated tables for the second rotation must have
	// been created — the same bug silently skipped the CREATE TABLE
	// statement that precedes the sqle_rotations INSERT.
	for _, rt := range []string{"_202402", "_202403"} {
		var id int64
		err := m.dbs[0].QueryRow("SELECT id FROM monthly_users"+rt+" WHERE id=?", 0).Scan(&id)
		require.ErrorIs(t, err, sql.ErrNoRows,
			"monthly_users%s table must exist (issue #74)", rt)
	}
}

// txOptCapture wraps a *sql.DB so every BeginTx call is recorded. The
// remaining Database methods are promoted from the embedded *sql.DB so
// the wrapper still satisfies sqle.Database.
type txOptCapture struct {
	*sql.DB
	mu   sync.Mutex
	opts []*sql.TxOptions
}

func (c *txOptCapture) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	c.mu.Lock()
	c.opts = append(c.opts, opts)
	c.mu.Unlock()
	return c.DB.BeginTx(ctx, opts)
}

func (c *txOptCapture) snapshot() []*sql.TxOptions {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sql.TxOptions, len(c.opts))
	copy(out, c.opts)
	return out
}

// Regression test for issue #80: the migrator previously hard-coded
// `db.Transaction(ctx, nil, ...)` inside both startMigrate and
// startRotate, leaving callers no way to set an isolation level or the
// read-only flag. WithTxOptions should forward the supplied *sql.TxOptions
// to every BeginTx call. The wrapping txOptCapture inspects what
// reaches the driver layer.
func TestMigrate_WithTxOptions_Propagated(t *testing.T) {
	db, clean, err := createSqlite3()
	require.NoError(t, err)
	defer clean()

	capture := &txOptCapture{DB: db}

	want := &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false}
	m := New(sqle.Open(capture))
	require.NoError(t, m.Discover(fstest.MapFS{
		"0.1.0/1_create_table_users.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS users (id int NOT NULL, PRIMARY KEY (id));`),
		},
		"0.2.0/1_create_table_orders.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS orders (id int NOT NULL, PRIMARY KEY (id));`),
		},
	}, WithModule("tests"), WithTxOptions(want)))

	require.NoError(t, m.Init(context.TODO()))
	require.NoError(t, m.Migrate(context.TODO()))

	got := capture.snapshot()
	require.NotEmpty(t, got, "Migrate must open at least one transaction")
	for i, opts := range got {
		require.Same(t, want, opts,
			"BeginTx call #%d received %p, want %p (the exact *sql.TxOptions the caller passed via WithTxOptions, issue #80)",
			i, opts, want)
	}
}

// Companion of TestMigrate_WithTxOptions_Propagated for the rotation
// path: startRotate previously hard-coded `db.Transaction(ctx, nil, ...)`
// too, so a configured TxOptions must reach the rotation transactions as
// well.
func TestRotate_WithTxOptions_Propagated(t *testing.T) {
	db, clean, err := createSqlite3()
	require.NoError(t, err)
	defer clean()

	capture := &txOptCapture{DB: db}

	want := &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false}
	m := New(sqle.Open(capture))
	m.now = func() time.Time {
		return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	}

	require.NoError(t, m.Discover(fstest.MapFS{
		"monthly/monthly_logs.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
				id int NOT NULL,
				PRIMARY KEY (id)
			);`),
		},
	}, WithTxOptions(want)))

	require.NoError(t, m.Init(context.TODO()))
	require.NoError(t, m.Rotate(context.TODO()))

	got := capture.snapshot()
	require.NotEmpty(t, got, "Rotate must open at least one transaction")
	for i, opts := range got {
		require.Same(t, want, opts,
			"BeginTx call #%d received %p, want %p (the exact *sql.TxOptions the caller passed via WithTxOptions, issue #80)",
			i, opts, want)
	}
}

// Default (no WithTxOptions) keeps the previous behaviour: the migrator
// passes nil to db.Transaction, which in turn passes nil to BeginTx.
// This guards against a future change that would silently start
// constructing a non-nil *sql.TxOptions{} by accident.
func TestMigrate_NoTxOptions_PassesNilToBeginTx(t *testing.T) {
	db, clean, err := createSqlite3()
	require.NoError(t, err)
	defer clean()

	capture := &txOptCapture{DB: db}

	m := New(sqle.Open(capture))
	require.NoError(t, m.Discover(fstest.MapFS{
		"0.1.0/1_create_table_users.sql": &fstest.MapFile{
			Data: []byte(`CREATE TABLE IF NOT EXISTS users (id int NOT NULL, PRIMARY KEY (id));`),
		},
	}, WithModule("tests")))

	require.NoError(t, m.Init(context.TODO()))
	require.NoError(t, m.Migrate(context.TODO()))

	for i, opts := range capture.snapshot() {
		require.Nil(t, opts,
			"BeginTx call #%d must receive nil TxOptions by default; got %+v (issue #80 regression check)",
			i, opts)
	}
}
