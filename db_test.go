package sqle

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle/shardid"
)

func createSQLite3() *sql.DB {
	// f, err := os.CreateTemp(".", "*.db")
	// f.Close()

	// clean := func() {
	// 	os.Remove(f.Name()) //nolint
	// }

	// if err != nil {
	// 	return nil, clean, err
	// }

	//db, err := sql.Open("sqlite3", "file:"+f.Name()+"?cache=shared")
	db, err := sql.Open("sqlite3", "file::memory:")

	if err != nil {
		return nil
	}
	//https://github.com/mattn/go-sqlite3/issues/209
	// db.SetMaxOpenConns(1)
	return db
}

func TestSharding(t *testing.T) {
	dbs := make([]*sql.DB, 0, 10)

	for i := 0; i < 10; i++ {
		db3 := createSQLite3()

		db3.Exec("CREATE TABLE `users` (`id` bigint , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))") //nolint: errcheck

		dbs = append(dbs, db3)
	}

	db := Open(dbs...)
	gen := shardid.New(shardid.WithDatabase(10))

	ids := make([]shardid.ID, 10)

	for i := 0; i < 10; i++ {
		id := gen.Next()
		b := New().On(id).
			Insert("users").
			Set("id", id.Int64).
			Set("status", 1).
			Set("created", time.Now()).
			End()
		result, err := db.On(id).ExecBuilder(context.TODO(), b)

		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		ids[i] = id
	}

	for i, id := range ids {
		b := New().On(id).Select("users", "id")

		ctx := db.On(id)

		require.Equal(t, i, ctx.index)

		var userID int64
		err := ctx.QueryRowBuilder(context.TODO(), b).Scan(&userID)
		require.NoError(t, err)
		require.Equal(t, id.Int64, userID)
	}

}
