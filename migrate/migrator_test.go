package migrate

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle"
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

				err := m.db.QueryRow("SELECT id,status,email,passwd,salt FROM members WHERE id=?", 0).Bind(&Member{})
				require.ErrorIs(t, err, sql.ErrNoRows)

				type Role struct {
					ID   int
					Name string
				}

				err = m.db.QueryRow("SELECT id,name FROM roles WHERE id=?", 0).Bind(&Role{})
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

				err := m.db.QueryRow("SELECT id,status,email,passwd,salt FROM members WHERE id=?", 0).Bind(&Member{})
				require.ErrorIs(t, err, sql.ErrNoRows)

				type Role struct {
					ID   int
					Name string
				}

				err = m.db.QueryRow("SELECT id,name FROM roles WHERE id=?", 0).Bind(&Role{})
				require.ErrorIs(t, err, sql.ErrNoRows)

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

			test.assert(t, m)
		})
	}

}
