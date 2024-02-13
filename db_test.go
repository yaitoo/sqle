package sqle

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func createSQLite3() (*sql.DB, func(), error) {
	f, err := os.CreateTemp(".", "*.db")
	f.Close()

	clean := func() {
		os.Remove(f.Name()) //nolint
	}

	if err != nil {
		return nil, clean, err
	}

	//db, err := sql.Open("sqlite3", "file:"+f.Name()+"?cache=shared")

	db, err := sql.Open("sqlite3", "file::memory:")

	if err != nil {
		return nil, clean, err
	}
	//https://github.com/mattn/go-sqlite3/issues/209
	// db.SetMaxOpenConns(1)
	return db, clean, nil
}
