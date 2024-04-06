package sqle

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

type cachedStmt struct {
	sync.Mutex
	stmt     *sql.Stmt
	lastUsed time.Time
}

func (db *Context) prepareStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	db.stmtsMutex.Lock()
	defer db.stmtsMutex.Unlock()
	s, ok := db.stmts[query]

	if ok {
		s.lastUsed = time.Now()
		return s.stmt, nil
	}

	stmt, err := db.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	db.stmts[query] = &cachedStmt{
		stmt:     stmt,
		lastUsed: time.Now(),
	}

	return stmt, nil
}

func (db *Context) closeIdleStmt() {
	for {
		<-time.After(StmtMaxIdleTime)

		db.stmtsMutex.Lock()
		lastActive := time.Now().Add(-1 * time.Minute)
		for k, v := range db.stmts {
			if v.lastUsed.Before(lastActive) {
				delete(db.stmts, k)
				go v.stmt.Close() //nolint: errcheck
			}
		}
		db.stmtsMutex.Unlock()
	}
}
