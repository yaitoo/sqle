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

func getStmt(ctx context.Context, db *sql.DB, query string) (*sql.Stmt, error) {
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
		var queries []string
		var todo []*sql.Stmt
		stmtsMutex.RLock()
		lastUsed := time.Now().Add(-1 * time.Minute)
		for k, v := range stmts {
			if v.lastUsed.Before(lastUsed) {
				queries = append(queries, k)
				todo = append(todo, v.stmt)
			}
		}
		stmtsMutex.Unlock()

		stmtsMutex.Lock()
		for _, q := range queries {
			delete(stmts, q)
		}
		stmtsMutex.Unlock()

		for _, s := range todo {
			s.Close()
		}
	}
}
