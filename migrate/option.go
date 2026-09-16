package migrate

import (
	"database/sql"
	"strings"
)

type Option func(m *Migrator)

func WithSuffix(suffix string) Option {
	return func(m *Migrator) {
		if suffix != "" {
			if !strings.HasPrefix(suffix, ".") {
				m.suffix = "." + suffix
			} else {
				m.suffix = suffix
			}
		}
	}
}

func WithModule(name string) Option {
	return func(m *Migrator) {
		m.module = name
	}
}

// WithTxOptions configures the *sql.TxOptions used by Migrate and Rotate
// when opening the per-version and per-rotation transactions. A nil opts
// (or the zero value) preserves the previous behaviour of passing nil to
// db.Transaction.
//
// On databases whose default isolation makes parallel migrations
// deadlock-prone (MySQL REPEATABLE READ, PostgreSQL SERIALIZABLE), call
// sites typically want sql.LevelReadCommitted. Verification steps that
// only read existing state may also set ReadOnly to true.
//
// Example:
//
//	m := migrate.New(db,
//	    migrate.WithTxOptions(&sql.TxOptions{
//	        Isolation: sql.LevelReadCommitted,
//	    }),
//	)
func WithTxOptions(opts *sql.TxOptions) Option {
	return func(m *Migrator) {
		m.txOpts = opts
	}
}
