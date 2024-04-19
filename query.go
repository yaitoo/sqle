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
	db                *DB
	queryer           Queryer[T]
	withRotatedTables []string
}

// NewQuery creates a new Query instance.
// It takes a *DB as the first argument and optional QueryOption functions as the rest.
// It returns a pointer to the created Query instance.
func NewQuery[T any](db *DB, options ...QueryOption[T]) *Query[T] {
	q := &Query[T]{
		db: db,
	}

	for _, opt := range options {
		if opt != nil {
			opt(q)
		}
	}

	if q.withRotatedTables == nil {
		q.withRotatedTables = []string{""}
	}

	if q.queryer == nil {
		q.queryer = &MapR[T]{
			dbs: q.db.dbs,
		}
	}

	return q
}

// First executes the query and returns the first result.
// It takes a context.Context and a *Builder as arguments.
// It returns the result of type T and an error, if any.
func (q *Query[T]) First(ctx context.Context, b *Builder) (T, error) {
	return q.queryer.First(ctx, q.withRotatedTables, b)
}

// Count executes the query and returns the number of results.
// It takes a context.Context and a *Builder as arguments.
// It returns the count as an integer and an error, if any.
func (q *Query[T]) Count(ctx context.Context, b *Builder) (int64, error) {
	return q.queryer.Count(ctx, q.withRotatedTables, b)
}

// Query executes the query and returns all the results.
// It takes a context.Context, a *Builder, and a comparison function as arguments.
// The comparison function is used to sort the results.
// It returns a slice of results of type T and an error, if any.
func (q *Query[T]) Query(ctx context.Context, b *Builder, less func(i, j T) bool) ([]T, error) {
	return q.queryer.Query(ctx, q.withRotatedTables, b, less)
}

// QueryLimit executes the query and returns a limited number of results.
// It takes a context.Context, a *Builder, a comparison function, and a limit as arguments.
// The comparison function is used to sort the results.
// The limit specifies the maximum number of results to return.
// It returns a slice of results of type T and an error, if any.
func (q *Query[T]) QueryLimit(ctx context.Context, b *Builder, less func(i, j T) bool, limit int) ([]T, error) {
	return q.queryer.QueryLimit(ctx, q.withRotatedTables, b, less, limit)
}
