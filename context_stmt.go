package sqle

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

type Stmt struct {
	*sql.Stmt
	mu       sync.Mutex
	lastUsed time.Time
	isUsing  bool
}

func (s *Stmt) Reuse() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.isUsing = false
}

func (db *Context) prepareStmt(ctx context.Context, query string) (*Stmt, error) {
	db.stmtsMutex.Lock()
	defer db.stmtsMutex.Unlock()
	s, ok := db.stmts[query]

	if ok {
		s.lastUsed = time.Now()
		s.isUsing = true
		return s, nil
	}

	stmt, err := db.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	s = &Stmt{
		Stmt:     stmt,
		lastUsed: time.Now(),
		isUsing:  true,
	}

	db.stmts[query] = s

	return s, nil
}

func (db *Context) closeIdleStmt() {
	for {
		<-time.After(StmtMaxIdleTime)

		db.stmtsMutex.Lock()
		lastActive := time.Now().Add(-1 * time.Minute)
		for k, s := range db.stmts {
			s.mu.Lock()
			if !s.isUsing && s.lastUsed.Before(lastActive) {
				delete(db.stmts, k)
				go s.Stmt.Close() //nolint: errcheck
			}
			s.mu.Unlock()
		}
		db.stmtsMutex.Unlock()
	}
}
