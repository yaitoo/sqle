package sqle

import (
	"context"
	"sort"
	"strconv"
	"strings"

	"github.com/yaitoo/async"
)

// MapR Map/Reduce Query
type MapR[T any] struct {
	dbs []*Context
}

func (q *MapR[T]) First(ctx context.Context, tables []string, b *Builder) (T, error) {
	var it T
	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return it, err
	}

	w := async.New[T]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Context, qr string) func(context.Context) (T, error) {
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
func (q *MapR[T]) Count(ctx context.Context, tables []string, b *Builder) (int, error) {
	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return 0, err
	}

	w := async.New[int]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Context, qr string) func(context.Context) (int, error) {
				return func(ctx context.Context) (int, error) {
					var i int
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

	var total int

	for _, it := range items {
		total += it
	}

	return total, nil
}
func (q *MapR[T]) Query(ctx context.Context, tables []string, b *Builder, less func(i, j T) bool) ([]T, error) {

	b.Input("rotate", "<rotate>") // lazy replace on async.Wait
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	w := async.New[[]T]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "<rotate>", r)
		for _, db := range q.dbs {
			w.Add(func(db *Context, qr string) func(context.Context) ([]T, error) {
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
func (q *MapR[T]) QueryLimit(ctx context.Context, tables []string, b *Builder, less func(i, j T) bool, limit int) ([]T, error) {

	if limit > 0 {
		b.SQL(" LIMIT " + strconv.Itoa(limit*len(q.dbs)))
	}

	list, err := q.Query(ctx, tables, b, less)
	if err != nil {
		return nil, err
	}

	if limit < len(list) {
		return list[0:limit], nil
	}

	return list, nil
}
