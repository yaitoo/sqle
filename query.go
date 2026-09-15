package sqle

import (
	"context"
	"errors"
	"fmt"
)

// Errors aggregates multiple errors so a caller can observe every failure
// from a fan-out operation rather than only the first one. It implements
// the multi-error unwrap contract (Unwrap []error) so callers can use
// errors.Is / errors.As to inspect any individual error, including
// sentinel values such as sql.ErrNoRows, sql.ErrTxDone, or context.Canceled.
type Errors struct {
	items []error
}

func (e *Errors) Error() string {
	return fmt.Sprint(e.items)
}

// Unwrap returns the aggregated errors so that errors.Is and errors.As
// traverse every contained error (Go 1.20+ multi-error semantics).
// It returns nil when there are no items.
func (e *Errors) Unwrap() []error {
	if e == nil || len(e.items) == 0 {
		return nil
	}
	return e.items
}

// Is reports whether any error in the aggregate matches target, using
// errors.Is semantics on each item. This lets callers branch on sentinels
// (sql.ErrNoRows, context.Canceled, ...) directly against an *Errors value
// even before the standard library's multi-error unwrap kicks in.
func (e *Errors) Is(target error) bool {
	if e == nil {
		return target == nil
	}
	for _, it := range e.items {
		if errors.Is(it, target) {
			return true
		}
	}
	return false
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
