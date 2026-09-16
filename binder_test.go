package sqle

import (
	"context"
	"database/sql"
	"runtime"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type UserWithUnderscoreTag struct {
	ID        int    `db:"id"`
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string `db:"email"`
}

type UserWithoutTag struct {
	ID        int
	FirstName string
	LastName  string
	Email     string
}

func TestStructBinderWithUnderscoreTags(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	defer d.Close()

	_, err = d.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, email TEXT)")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO users (id, first_name, last_name, email) VALUES (1, 'John', 'Doe', 'john@example.com')")
	require.NoError(t, err)

	db := Open(d)

	t.Run("db_tag_with_underscores_should_work", func(t *testing.T) {
		var user UserWithUnderscoreTag
		err := db.QueryRow("SELECT id, first_name, last_name, email FROM users WHERE id = 1").Bind(&user)
		require.NoError(t, err)
		require.Equal(t, 1, user.ID)
		require.Equal(t, "John", user.FirstName)
		require.Equal(t, "Doe", user.LastName)
		require.Equal(t, "john@example.com", user.Email)
	})

	t.Run("without_tag_should_work", func(t *testing.T) {
		var user UserWithoutTag
		err := db.QueryRow("SELECT id, first_name, last_name, email FROM users WHERE id = 1").Bind(&user)
		require.NoError(t, err)
		require.Equal(t, 1, user.ID)
		require.Equal(t, "John", user.FirstName)
		require.Equal(t, "Doe", user.LastName)
		require.Equal(t, "john@example.com", user.Email)
	})

	t.Run("uppercase_column_names_should_work", func(t *testing.T) {
		var user UserWithUnderscoreTag
		err := db.QueryRow("SELECT id, FIRST_NAME, LAST_NAME, EMAIL FROM users WHERE id = 1").Bind(&user)
		require.NoError(t, err)
		require.Equal(t, 1, user.ID)
		require.Equal(t, "John", user.FirstName)
		require.Equal(t, "Doe", user.LastName)
		require.Equal(t, "john@example.com", user.Email)
	})

	t.Run("mixed_case_column_names_should_work", func(t *testing.T) {
		var user UserWithUnderscoreTag
		err := db.QueryRow("SELECT id, First_Name, Last_Name, Email FROM users WHERE id = 1").Bind(&user)
		require.NoError(t, err)
		require.Equal(t, 1, user.ID)
		require.Equal(t, "John", user.FirstName)
		require.Equal(t, "Doe", user.LastName)
		require.Equal(t, "john@example.com", user.Email)
	})

	t.Run("slice_binding_should_work", func(t *testing.T) {
		_, err = d.Exec("INSERT INTO users (id, first_name, last_name, email) VALUES (2, 'Jane', 'Smith', 'jane@example.com')")
		require.NoError(t, err)

		var users []UserWithUnderscoreTag
		rows, err := db.QueryBuilder(context.Background(), New("SELECT id, first_name, last_name, email FROM users ORDER BY id"))
		require.NoError(t, err)
		err = rows.Bind(&users)
		require.NoError(t, err)
		require.Len(t, users, 2)
		require.Equal(t, 1, users[0].ID)
		require.Equal(t, "John", users[0].FirstName)
		require.Equal(t, 2, users[1].ID)
		require.Equal(t, "Jane", users[1].FirstName)
	})
}

// TestMapBinderPointerFieldsSurviveGC is a regression test for the
// noscan-corruption hazard that the dsh code review flagged in
// mapBinder.acquire (issue #78): a typed scratch slice must keep the GC
// bitmap accurate so pointer fields written by rows.Scan (string data,
// time.Time *Location, sql.NullString fields, interface headers) remain
// reachable for the lifetime of the row.
//
// We scan many rows into maps whose element types contain pointers and
// force a GC between rows. If the backing store were noscan ([]byte) the
// GC could free pointer-typed fields under pressure and the next scan
// would observe corrupted values.
func TestMapBinderPointerFieldsSurviveGC(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	defer d.Close()

	_, err = d.Exec(`CREATE TABLE messages (
		id INTEGER PRIMARY KEY,
		subject TEXT,
		body TEXT,
		sender TEXT
	)`)
	require.NoError(t, err)

	const rows = 200
	for i := 0; i < rows; i++ {
		_, err = d.Exec(
			"INSERT INTO messages (id, subject, body, sender) VALUES (?, ?, ?, ?)",
			i,
			"subject "+string(rune('A'+i%26)),
			"body for row "+string(rune('A'+i%26)),
			"sender@example.com",
		)
		require.NoError(t, err)
	}

	db := Open(d)

	t.Run("map_string_string", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			var list []map[string]string
			rows, err := db.Query("SELECT id, subject, body, sender FROM messages ORDER BY id LIMIT 50")
			require.NoError(t, err)
			require.NoError(t, rows.Bind(&list))
			require.Len(t, list, 50)
			require.Equal(t, "subject A", list[0]["subject"])
			require.Equal(t, "sender@example.com", list[0]["sender"])
			runtime.GC()
		}
	})

	t.Run("map_string_any", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			var list []map[string]any
			rows, err := db.Query("SELECT id, subject, body, sender FROM messages ORDER BY id LIMIT 50")
			require.NoError(t, err)
			require.NoError(t, rows.Bind(&list))
			require.Len(t, list, 50)
			require.Equal(t, "sender@example.com", list[0]["sender"])
			runtime.GC()
		}
	})

	t.Run("list_of_map_string_string", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			var list []map[string]string
			rows, err := db.Query("SELECT id, subject, body, sender FROM messages ORDER BY id LIMIT 50")
			require.NoError(t, err)
			require.NoError(t, rows.Bind(&list))
			require.Len(t, list, 50)
			for _, m := range list {
				require.Equal(t, "sender@example.com", m["sender"])
			}
			runtime.GC()
		}
	})
}

// TestScanToListPrimitiveStringValuesSurviveGC covers the same hazard on
// the scanToList primitive branch — the unsafe-free reflect.Copy and the
// typed scratch slice must keep string values written into each per-row
// []string alive across iterations and across GCs.
func TestScanToListPrimitiveStringValuesSurviveGC(t *testing.T) {
	d, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	defer d.Close()

	_, err = d.Exec(`CREATE TABLE strings (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)

	for i := 0; i < 32; i++ {
		_, err = d.Exec("INSERT INTO strings (id, v) VALUES (?, ?)", i, "row-"+string(rune('A'+i%26)))
		require.NoError(t, err)
	}

	db := Open(d)

	for i := 0; i < 20; i++ {
		var out [][]string
		rows, err := db.Query("SELECT id, v FROM strings ORDER BY id")
		require.NoError(t, err)
		require.NoError(t, rows.Bind(&out))
		require.Len(t, out, 32)
		// Every row should still hold its own distinct string after
		// the GC pass — the typed-scratch backing of scanToList
		// primitive branch must keep each row's string value alive.
		require.Equal(t, "row-A", out[0][1])
		require.Equal(t, "row-P", out[15][1])
		runtime.GC()
	}

	// Also exercise time.Time / sql.Scanner element types, which hold
	// internal pointers (*Location, []byte payload).
	d2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)
	defer d2.Close()
	_, err = d2.Exec(`CREATE TABLE stamps (id INTEGER PRIMARY KEY, ts DATETIME)`)
	require.NoError(t, err)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 16; i++ {
		_, err = d2.Exec("INSERT INTO stamps (id, ts) VALUES (?, ?)", i, base.Add(time.Duration(i)*time.Hour))
		require.NoError(t, err)
	}
	db2 := Open(d2)
	for i := 0; i < 20; i++ {
		var out [][]time.Time
		rows, err := db2.Query("SELECT ts FROM stamps ORDER BY id")
		require.NoError(t, err)
		require.NoError(t, rows.Bind(&out))
		require.Len(t, out, 16)
		require.True(t, out[0][0].Equal(base))
		runtime.GC()
	}

	// Suppress unused import warnings.
	_ = context.Background
}
