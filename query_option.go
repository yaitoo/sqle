package sqle

import (
	"time"

	"github.com/yaitoo/sqle/shardid"
)

type QueryOption[T any] func(q *Query[T])

func WithMonths[T any](start, end time.Time) QueryOption[T] {
	return func(q *Query[T]) {
		for t := start; !t.After(end); t = t.AddDate(0, 1, 0) {
			q.tables = append(q.tables, "_"+shardid.FormatMonth(t))
		}
	}
}

func WithWeeks[T any](start, end time.Time) QueryOption[T] {
	return func(q *Query[T]) {
		for t := start; !t.After(end); t = t.AddDate(0, 0, 7) {
			q.tables = append(q.tables, "_"+shardid.FormatWeek(t))
		}
	}
}

func WithDays[T any](start, end time.Time) QueryOption[T] {
	return func(q *Query[T]) {
		for t := start; !t.After(end); t = t.AddDate(0, 0, 1) {
			q.tables = append(q.tables, "_"+shardid.FormatDay(t))
		}
	}
}

func WithQueryer[T any](qr Queryer[T]) QueryOption[T] {
	return func(q *Query[T]) {
		q.queryer = qr
	}
}
