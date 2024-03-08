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
	query, args, err := b.Build()
	if err != nil {
		return it, err
	}

	w := async.New[T]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "_rotate", r)
		for _, db := range q.dbs {
			w.Add(func(context.Context) (T, error) {
				var t T
				err := db.QueryRowContext(ctx, qr, args...).Bind(&t)
				if err != nil {
					return t, err
				}

				return t, nil
			})
		}
	}

	return w.WaitAny(ctx)
}
func (q *MapR[T]) Count(ctx context.Context, tables []string, b *Builder) (int, error) {

	query, args, err := b.Build()
	if err != nil {
		return 0, err
	}

	w := async.New[int]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "_rotate", r)
		for _, db := range q.dbs {
			w.Add(func(context.Context) (int, error) {
				var i int
				err := db.QueryRowContext(ctx, qr, args...).Bind(&i)
				if err != nil {
					return i, err
				}

				return i, nil
			})
		}
	}

	items, err := w.Wait(ctx)

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
	query, args, err := b.Build()
	if err != nil {
		return nil, err
	}

	w := async.New[T]()

	for _, r := range tables {
		qr := strings.ReplaceAll(query, "_rotate", r)
		for _, db := range q.dbs {
			w.Add(func(context.Context) (T, error) {
				var t T
				err := db.QueryRowContext(ctx, qr, args...).Bind(&t)
				if err != nil {
					return t, err
				}

				return t, nil
			})
		}
	}

	items, err := w.Wait(ctx)

	if err != nil {
		return nil, err
	}

	var list []T

	for _, it := range items {
		list = append(list, it)
	}

	if less != nil {
		sort.Slice(list, func(i, j int) bool {
			return less(list[i], list[j])
		})
	}

	return list, nil
}
func (q *MapR[T]) QueryLimit(ctx context.Context, tables []string, b *Builder, less func(i, j T) bool, limit int, offset int) ([]T, error) {

	if limit > 0 {
		b.SQL(" LIMIT " + strconv.Itoa(limit))
	}

	if offset > 0 {
		b.SQL(" OFFSET " + strconv.Itoa(offset))
	}

	list, err := q.Query(ctx, tables, b, less)
	if err != nil {
		return nil, err
	}

	return list[0:limit], nil
}
