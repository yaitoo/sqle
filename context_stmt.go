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
	db.stmtsMutex.RLock()
	s, ok := db.stmts[query]
	db.stmtsMutex.RUnlock()
	if ok {
		s.Lock()
		s.lastUsed = time.Now()
		s.Unlock()
		return s.stmt, nil
	}

	stmt, err := db.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	db.stmtsMutex.Lock()
	db.stmts[query] = &cachedStmt{
		stmt:     stmt,
		lastUsed: time.Now(),
	}
	db.stmtsMutex.Unlock()

	return stmt, nil
}

func (db *Context) closeIdleStmt() {
	for {
		<-time.After(1 * time.Minute)

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
