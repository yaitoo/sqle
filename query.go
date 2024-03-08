package sqle

import (
	"context"
	"fmt"
)

type Errors struct {
	items []error
}

func (e *Errors) Error() string {
	return fmt.Sprint(e.items)
}

type Query[T any] struct {
	db      *DB
	queryer Queryer[T]
	tables  []string
}

// NewQuery create a Query
func NewQuery[T any](db *DB, options ...QueryOption[T]) Query[T] {
	q := Query[T]{
		db: db,
	}

	for _, opt := range options {
		if opt != nil {
			opt(&q)
		}
	}

	if q.tables == nil {
		q.tables = []string{""}
	}

	if q.queryer == nil {
		q.queryer = &MapR[T]{
			dbs: q.db.dbs,
		}
	}

	return q
}

func (q *Query[T]) First(ctx context.Context, b *Builder) (T, error) {
	return q.queryer.First(ctx, q.tables, b)
}

func (q *Query[T]) Count(ctx context.Context, b *Builder) (int, error) {
	return q.queryer.Count(ctx, q.tables, b)
}

func (q *Query[T]) Query(ctx context.Context, b *Builder, less func(i, j T) bool) ([]T, error) {
	return q.queryer.Query(ctx, q.tables, b, less)
}

func (q *Query[T]) QueryLimit(ctx context.Context, b *Builder, less func(i, j T) bool, limit int, offset int) ([]T, error) {
	return q.queryer.QueryLimit(ctx, q.tables, b, less, limit, offset)
}
