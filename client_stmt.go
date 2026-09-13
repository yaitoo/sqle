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
	// refCount tracks the number of in-flight *sql.Rows / Row operations
	// that are still using this *sql.Stmt. closeStaleStmt must not close
	// a Stmt while refCount > 0, otherwise the driver returns
	// "sql: statement is closed" on rows.Next / rows.Scan.
	refCount int
}

// Acquire marks the statement as in use by an in-flight operation. Every
// successful prepareStmt increments refCount; the matching release happens
// via Reuse, typically from Rows.Close or Row.Close. Acquire also refreshes
// lastUsed and sets isUsing while refCount > 0.
func (s *Stmt) Acquire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refCount++
	s.isUsing = true
	s.lastUsed = time.Now()
}

// Reuse releases one in-flight reference. When refCount drops to zero the
// statement becomes idle (isUsing = false) and may be closed by
// closeStaleStmt. Reuse is idempotent beyond refCount == 0 — extra calls
// are no-ops, which keeps double-close paths safe.
func (s *Stmt) Reuse() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.refCount > 0 {
		s.refCount--
	}
	if s.refCount == 0 {
		s.isUsing = false
	}
}

func (db *Client) prepareStmt(ctx context.Context, query string) (*Stmt, error) {
	db.stmtsMutex.Lock()
	defer db.stmtsMutex.Unlock()
	s, ok := db.stmts[query]

	if ok {
		s.Acquire()
		return s, nil
	}

	stmt, err := db.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	s = &Stmt{Stmt: stmt}
	s.Acquire() // refCount = 1, isUsing = true, lastUsed = now

	db.stmts[query] = s

	return s, nil
}

func (db *Client) closeStaleStmt() {
	db.stmtsMutex.Lock()
	defer db.stmtsMutex.Unlock()

	lastActive := time.Now().Add(-db.stmtMaxIdleTime)
	for k, s := range db.stmts {
		s.mu.Lock()
		if !s.isUsing && s.lastUsed.Before(lastActive) {
			delete(db.stmts, k)
			go s.Stmt.Close() //nolint: errcheck
		}
		s.mu.Unlock()
	}

}

func (db *Client) checkIdleStmt() {
	delay := time.NewTicker(db.stmtMaxIdleTime)
	defer delay.Stop()

	for {
		<-delay.C

		db.closeStaleStmt()
	}
}
