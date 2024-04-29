package sqle

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/yaitoo/async"
)

// MapR is a Map/Reduce Query Provider based on databases.
type MapR[T any] struct {
	dbs []*Client
}

// First executes the query and returns the first result.
func (q *MapR[T]) First(ctx context.Context, rotatedTables []string, b *Builder) (T, error) {
	var it T
	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return it, err
	}

	w := async.New[T]()

	for _, r := range rotatedTables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Client, qr string) func(context.Context) (T, error) {
				return func(ctx context.Context) (T, error) {
					var t T
					err := db.QueryRowContext(ctx, qr, args...).Bind(&t)
					if err != nil {
						return t, err
					}

					return t, nil
				}
			}(db, qr))
		}
	}

	d, _, err := w.WaitAny(ctx)
	return d, err
}

// Count executes the query and returns the count of results.
func (q *MapR[T]) Count(ctx context.Context, rotatedTables []string, b *Builder) (int64, error) {
	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return 0, err
	}

	w := async.New[int64]()

	for _, r := range rotatedTables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Client, qr string) func(context.Context) (int64, error) {
				return func(ctx context.Context) (int64, error) {
					var i int64
					err := db.QueryRowContext(ctx, qr, args...).Scan(&i)
					if err != nil {
						return i, err
					}

					return i, nil
				}
			}(db, qr))
		}
	}

	items, _, err := w.Wait(ctx)

	if err != nil {
		return 0, err
	}

	var total int64

	for _, it := range items {
		total += it
	}

	return total, nil
}

// Query executes the query and returns a list of results.
func (q *MapR[T]) Query(ctx context.Context, rotatedTables []string, b *Builder, less func(i, j T) bool) ([]T, error) {

	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	w := async.New[[]T]()

	for _, r := range rotatedTables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Client, qr string) func(context.Context) ([]T, error) {
				return func(context.Context) ([]T, error) {
					var t []T
					rows, err := db.QueryContext(ctx, qr, args...)
					if err != nil {
						return t, err
					}

					err = rows.Bind(&t)
					if err != nil {
						return t, err
					}

					return t, nil
				}
			}(db, qr))
		}
	}

	items, _, err := w.Wait(ctx)

	if err != nil {
		return nil, err
	}

	var list []T

	for _, it := range items {
		if it != nil {
			list = append(list, it...)
		}
	}

	if less != nil {
		sort.Slice(list, func(i, j int) bool {
			return less(list[i], list[j])
		})
	}

	return list, nil
}

// QueryLimit executes the query and returns a limited list of results.
func (q *MapR[T]) QueryLimit(ctx context.Context, rotatedTables []string, b *Builder, less func(i, j T) bool, limit int) ([]T, error) {

	if limit > 0 {
		b.SQL(" LIMIT " + strconv.Itoa(limit*len(q.dbs)))
	}

	list, err := q.Query(ctx, rotatedTables, b, less)
	if err != nil {
		return nil, err
	}

	if limit < len(list) {
		return list[0:limit], nil
	}

	return list, nil
}
