package sqle

import (
	"context"
	"database/sql"
	"testing"

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
