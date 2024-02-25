package sqle

import (
	"database/sql"

	"github.com/yaitoo/sqle/shardid"
)

type DB struct {
	*Context
	_ noCopy //nolint: unused

	dbs []*Context
}

func Open(dbs ...*sql.DB) *DB {
	d := &DB{
		Context: &Context{
			DB:    dbs[0],
			stmts: make(map[string]*cachedStmt),
		},
	}

	for i, db := range dbs {
		ctx := &Context{
			DB:    db,
			index: i,
			stmts: make(map[string]*cachedStmt),
		}
		d.dbs = append(d.dbs, ctx)
		go ctx.closeIdleStmt()
	}

	return d
}

func (db *DB) On(id shardid.ID) *Context {
	return db.dbs[int(id.DatabaseID)]
}
