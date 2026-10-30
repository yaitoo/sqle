package sqle

import (
	"bytes"
	"database/sql"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countReapers returns the number of live statement reaper goroutines. The
// reaper runs in Client.checkIdleStmt, so it shows up in the goroutine dump
// by function name.
func countReapers() int {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte("checkIdleStmt"))
		}
		buf = make([]byte, 2*len(buf))
	}
}

// TestCloseStopsIdleStmtReaper verifies that closing a client terminates the
// background statement reaper it started. The reaper used to have no exit
// path, so every Open leaked a goroutine (and a ticker) for the lifetime of
// the process.
func TestCloseStopsIdleStmtReaper(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	before := countReapers()

	db := Open(d)
	client := db.Client

	// Open starts one reaper per wrapped database.
	require.Eventually(t, func() bool {
		return countReapers() == before+1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, client.Close())

	// Close must terminate the reaper instead of leaking it.
	require.Equal(t, before, countReapers())

	// The underlying database is closed as well.
	require.Error(t, client.Ping())
}

// TestStopKeepsDatabaseOpen verifies that Stop only terminates the reaper
// and leaves the underlying database usable.
func TestStopKeepsDatabaseOpen(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	before := countReapers()

	db := Open(d)
	client := db.Client

	require.Eventually(t, func() bool {
		return countReapers() == before+1
	}, 5*time.Second, 10*time.Millisecond)

	client.Stop()

	require.Equal(t, before, countReapers())
	require.NoError(t, client.Ping())

	// Stop is idempotent, and Close still closes the database afterwards.
	client.Stop()
	require.NoError(t, client.Close())
	require.Error(t, client.Ping())
}

func TestStmt(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `rows_stmt` (`id` int , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()

	_, err = d.Exec("INSERT INTO `rows_stmt`(`id`,`status`,`email`,`passwd`,`salt`,`created`) VALUES(1, 1,'test1@mail.com','1xxxx','1zzzz', ?)", now)
	require.NoError(t, err)
	_, err = d.Exec("INSERT INTO `rows_stmt`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(2, 2,'test2@mail.com','2xxxx','2zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows_stmt`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(3, 3,'test3@mail.com','3xxxx','3zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows_stmt`(`id`) VALUES(4)")
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
					rows, err := db.Query("SELECT * FROM rows_stmt WHERE id<?", 4)
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

					row := db.QueryRow("SELECT * FROM rows_stmt WHERE id=?", 1)
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

					result, err := db.Exec("INSERT INTO `rows_stmt`(`id`) VALUES(?)", i+100)
					require.NoError(t, err)
					rows, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, int64(1), rows)
				}

				rows, err := db.Query("SELECT id FROM rows_stmt WHERE id>=100 order by id")
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
				q := "INSERT INTO `rows_stmt`(`id`,`status`) VALUES(?, ?)"

				result, err := db.Exec(q, 200, 0)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)

				db.stmtsMutex.Lock()
				s, ok := db.stmts[q]
				db.stmtsMutex.Unlock()
				require.True(t, ok)
				require.False(t, s.refCount > 0)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt should be closed and released
				require.False(t, s.refCount > 0)

				s, ok = db.stmts[q]
				require.False(t, ok)
				require.Nil(t, s)

			},
		},
		{
			name: "stmt_reuse_should_work_in_rows_scan",
			run: func(t *testing.T) {
				var id int
				q := "SELECT id, 'rows_scan' as reuse FROM rows_stmt WHERE id = ?"
				rows, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				rows.Scan(&id) // nolint: errcheck
				require.True(t, s.refCount > 0)
				rows.Close()
				require.False(t, s.refCount > 0)

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

				q := "SELECT id, 'rows_bind' as reuse FROM rows_stmt WHERE id = ?"
				rows, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				rows.Bind(&r) // nolint: errcheck
				require.False(t, s.refCount > 0)

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
				q := "SELECT id, 'row_scan' as reuse FROM rows_stmt WHERE id = ?"
				row := db.QueryRow(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				row.Scan(&id) // nolint: errcheck
				require.False(t, s.refCount > 0)

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
				q := "SELECT id, 'row_bind' as reuse FROM rows_stmt WHERE id = ?"
				row, err := db.Query(q, 200)
				require.NoError(t, err)

				s, ok := db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				time.Sleep(2 * time.Second)
				db.closeStaleStmt()

				// stmt that is in using should not be closed
				s, ok = db.stmts[q]
				require.True(t, ok)
				require.True(t, s.refCount > 0)

				row.Bind(&r) // nolint: errcheck
				require.False(t, s.refCount > 0)

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
