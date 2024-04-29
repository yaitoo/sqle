package sqle

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStmt(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `rows` (`id` int , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()

	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`,`created`) VALUES(1, 1,'test1@mail.com','1xxxx','1zzzz', ?)", now)
	require.NoError(t, err)
	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(2, 2,'test2@mail.com','2xxxx','2zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(3, 3,'test3@mail.com','3xxxx','3zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows`(`id`) VALUES(4)")
	require.NoError(t, err)

	stmtMaxIdleTime := StmtMaxIdleTime

	StmtMaxIdleTime = 1 * time.Second
	db := Open(d)
	StmtMaxIdleTime = stmtMaxIdleTime

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "stmt_should_work_in_query",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}

				for i := 0; i < 100; i++ {
					rows, err := db.Query("SELECT * FROM rows WHERE id<?", 4)
					require.NoError(t, err)
					var users []user
					err = rows.Bind(&users)
					require.NoError(t, err)
					require.Len(t, users, 3)
					require.Equal(t, 1, users[0].ID)
					require.Equal(t, 2, users[1].ID)
					require.Equal(t, 3, users[2].ID)
				}

			},
		},
		{
			name: "stmt_should_work_in_query_row",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}

				for i := 0; i < 100; i++ {

					row := db.QueryRow("SELECT * FROM rows WHERE id=?", 1)
					require.NoError(t, row.err)
					var user user
					err = row.Bind(&user)
					require.NoError(t, err)
					require.Equal(t, 1, user.ID)
					require.Equal(t, "test1@mail.com", user.Email)
					require.Equal(t, "1xxxx", user.Passwd)
				}

			},
		},
		{
			name: "stmt_should_work",
			run: func(t *testing.T) {
				for i := 0; i < 100; i++ {

					result, err := db.Exec("INSERT INTO `rows`(`id`) VALUES(?)", i+100)
					require.NoError(t, err)
					rows, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, int64(1), rows)
				}

				rows, err := db.Query("SELECT id FROM rows WHERE id>=100 order by id")
				require.NoError(t, err)
				var list [][]int
				err = rows.Bind(&list)
				require.NoError(t, err)

				require.Len(t, list, 100)
				for i, id := range list {
					require.Equal(t, i+100, id[0])
				}
			},
		},
		{
			name: "stmt_reuse_should_work_in_exec",
			run: func(t *testing.T) {
				q := "INSERT INTO `rows`(`id`,`status`) VALUES(?, ?)"

				result, err := db.Exec(q, 200, 0)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)

				db.stmtsMutex.Lock()
				s, ok := db.stmts[q]
				db.stmtsMutex.Unlock()
				require.True(t, ok)
				require.False(t, s.isUsing)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt should be closed and released
				require.False(t, s.isUsing)

				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)

			},
		},
		{
			name: "stmt_reuse_should_work_in_rows_scan",
			run: func(t *testing.T) {
				var id int
				q := "SELECT id, 'rows_scan' as reuse FROM rows WHERE id = ?"
				rows, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				rows.Scan(&id) // nolint: errcheck
				require.False(t, s.isUsing)

				db.closeStaleStmt()

				// stmt should be closed and released
				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)
			},
		},
		{
			name: "stmt_reuse_should_work_in_rows_bind",
			run: func(t *testing.T) {
				var r struct {
					ID int
				}

				q := "SELECT id, 'rows_bind' as reuse FROM rows WHERE id = ?"
				rows, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				rows.Bind(&r) // nolint: errcheck
				require.False(t, s.isUsing)

				db.closeStaleStmt()

				// stmt should be closed and released
				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)
			},
		},
		{
			name: "stmt_reuse_should_work_in_row_scan",
			run: func(t *testing.T) {
				var id int
				q := "SELECT id, 'row_scan' as reuse FROM rows WHERE id = ?"
				row := db.QueryRow(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				row.Scan(&id) // nolint: errcheck
				require.False(t, s.isUsing)

				db.closeStaleStmt()

				// stmt should be closed and released
				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)
			},
		},
		{
			name: "stmt_reuse_should_work_in_row_bind",
			run: func(t *testing.T) {
				var r struct {
					ID int
				}
				q := "SELECT id, 'row_bind' as reuse FROM rows WHERE id = ?"
				row, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.isUsing)

				row.Bind(&r) // nolint: errcheck
				require.False(t, s.isUsing)

				db.closeStaleStmt()

				// stmt should be closed and released
				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t)
		})
	}
}
