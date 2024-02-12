package sqle

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

var (
	stmts      = make(map[string]*cachedStmt)
	stmtsMutex sync.RWMutex
)

type cachedStmt struct {
	sync.Mutex
	stmt     *sql.Stmt
	lastUsed time.Time
}

func prepareStmt(ctx context.Context, db *sql.DB, query string) (*sql.Stmt, error) {
	stmtsMutex.RLock()
	s, ok := stmts[query]
	stmtsMutex.RUnlock()
	if ok {
		s.Lock()
		s.lastUsed = time.Now()
		s.Unlock()
		return s.stmt, nil
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	stmtsMutex.Lock()
	stmts[query] = &cachedStmt{
		stmt:     stmt,
		lastUsed: time.Now(),
	}
	stmtsMutex.Unlock()

	return stmt, nil
}

func init() {
	go releaseCachedStmt()
}

func releaseCachedStmt() {
	for {
		<-time.After(1 * time.Minute)

		stmtsMutex.Lock()
		lastActive := time.Now().Add(-1 * time.Minute)
		for k, v := range stmts {
			if v.lastUsed.Before(lastActive) {
				delete(stmts, k)
				go v.stmt.Close() //nolint: errcheck
			}
		}
		stmtsMutex.Unlock()
	}
}
